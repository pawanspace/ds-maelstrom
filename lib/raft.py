#!/usr/bin/env python

import threading
from node import Node
from node import RPCError
from enum import Enum
import random
import time
import math

class Map():
    def __init__(self, data = dict()):
        self.data = data

    def apply(self, request, node):       
        op = request['body']
        key = op['key']
        match op['type']:
            case "read":
                if self.data.get(key):
                    return [self, self.generate_response(node, 'read_ok', request['src'], {"type": "read_ok", "value": self.data[key]})] 
                else: 
                    return [self, self.generate_response(node, 'error',  request['src'], RPCError.key_does_not_exist('not found').to_json())]
            case "write":
                updated_data = self.data.copy()
                updated_data[key] = op['value'] 
                return (Map(updated_data), self.generate_response(node, 'write_ok', request['src'], {"type": "write_ok"}))
            case "cas":
                if self.data.get(key):
                    if self.data[key] == op['from']:
                        updated_data = self.data.copy()
                        updated_data[key] = op['to']
                        return (Map(updated_data), self.generate_response(node, 'cas_ok', request['src'], {"type": "cas_ok"}))
                    else:
                        return [self, self.generate_response(node, 'error',  request['src'], RPCError.precondition_failed(f"expected {op['from']} but got {self.data[key]}").to_json())]
                else:
                    return [self, self.generate_response(node, 'error',  request['src'], RPCError.key_does_not_exist('not found').to_json())]
            case _:
                raise Exception("Unknown operation type")
    
    def generate_response(self, node, type, dest, body):
            return node.generate_response(type, dest, body)
            

# Stores Raft entries, which are maps with a {:term} field. Not thread-safe; we
# handle locking in the Raft class.
class Log():
    def __init__(self, node):
        self.node = node
        self.entries = [{"term": 0, "op": None}]

    def __getitem__(self, index):        
        try:
            return self.entries[index - 1]
        except(IndexError):
            return None
    
    def append(self, entries):
        self.entries.extend(entries)
        #self.node.log(f"Log: contains {self.entries} entries")

    def last(self):
        return self.entries[-1]

    def size(self):
        return len(self.entries)
    
    #Log truncation is important--we need to delete all mismatching log entries after the given index.
    def truncate(self, index):
        self.entries = self.entries[:index]
    
    def from_index(self, index):
        if index <= 0:
            raise Exception(f"Illegal index {index}")
            
        return self.entries[index-1:]

class State(Enum):
    Follower = 1,
    Candidate = 2,
    Leader = 3

class Raft():
    def __init__(self):
        # Components
        self.node = Node()
        self.lock = threading.RLock()
        self.state_machine = Map()
        self.log = Log(self.node)

        # Raft state
        self.state = State.Follower # Either follower, candidate, or leader      
        self.term = 0 # What's our current term?        
        self.voted_for = None # Which node did we vote for in this term?
        self.leader = None # Who is the leader?

        # Heartbeats and timeouts
        self.election_timeout = 2 # Time before next election, in seconds
        self.heart_beat_interval = 1 # Time between heartbeats, in seconds
        self.min_replication_interval = 0.05 # Don't replicate TOO frequently

        self.election_deadline = self.node.now()  # Next election, in epoch seconds
        self.stepdown_deadline = self.node.now() # When to step down automatically.
        self.last_replication = self.node.now() # When did we last replicate?
      
        # Leader state
        self.commit_index = 0 # The highest committed entry in the log
        self.next_index = None # A map of nodes to the next index to replicate
        self.match_index = None # A map of (other) nodes to the highest log entry
                                # known to be replicated on that node.


        self.last_applied = 1


        self.node.handlers['read'] = lambda request: self.client_req(request)
        self.node.handlers['write'] = lambda request: self.client_req(request)
        self.node.handlers['cas'] = lambda request: self.client_req(request)
        self.node.handlers['request_vote'] = lambda request: self.grant_vote(request)
        self.node.handlers['append_entries'] = lambda request: self.handle_append_entries(request)


        self.node.every(lambda: self.leader_heart_beat(), 0.1)
        self.node.every(lambda: self.heart_beat(), self.heart_beat_interval)
        self.node.every(lambda: self.replicate_log(False), self.min_replication_interval)
        

    def get_match_index(self):
        return self.match_index | {self.node.node_id: self.log.size()}


    def advance_state_machine(self):
        with self.lock:
            while self.last_applied < self.commit_index:
                #advance the applied index and apply that op
                self.last_applied += 1
                request = self.log[self.last_applied]['op']
                self.node.log(f"Applying {request}")
                self.state_machine, response = self.state_machine.apply(request, self.node)
                self.node.log(f"State machine resonse: {response}")

                if self.state == State.Leader:
                    # we are currently the leader, so we need to send a response to the client
                    self.node.reply(response, request)


    def handle_append_entries(self, request):
        with self.lock:
            body = request['body']
            self.maybe_step_down(body['term'])

            response_body  = {
                'type': 'append_entries_res',
                'term': self.term,
                'success': False
            }

            if body['term'] < self.term:
                self.node.log(f"Ignoring {body} because it's in a later term")
                response = self.node.generate_response('append_entries_res', request['src'], response_body)
                self.node.reply(response, request)
                return

            # valid leader lets reset election deadline
            self.reset_election_deadline()

            # Check previous entry to see if it matches
            if body['prev_log_index'] <= 0:
                raise RPCError.abort(f"Out of bounds previous log index {body['prev_log_index']}")

            previous_log_entry = self.log[body['prev_log_index']]

            # If the previous entry doesn't exist, or if we disagree on its term, we'll reject this request
            if previous_log_entry == None or previous_log_entry['term'] != body['prev_log_term']:
                # We disagree on the previous term
                self.node.log(f"We disagree on the previous term {previous_log_entry}")
                response = self.node.generate_response('append_entries_res', request['src'], response_body)
                self.node.reply(response, request)
                self.leader = body['leader_id']
                return
            
            # We agree on the previous log term; truncate and append
            # lets remove all entries from the previous log index
            # those entries are not not correct anymore according to leader.
            #Log truncation is important--we need to delete all mismatching log entries after the given index.
            self.log.truncate(body['prev_log_index'])
            self.log.append(body['entries'])

            # advance commit pointer
            if self.commit_index  < body['leader_commit']:
                self.commit_index = min(self.log.size(), body['leader_commit'])
            # for followers
            self.advance_state_machine()

            # Ack the replication
            response_body['success'] = True
            response = self.node.generate_response('append_entries_res', request['src'], response_body)
            self.node.reply(response, request)



            


    def replicate_log(self, force):
        with self.lock:            
            # How long has it been since we last replicated?
            elapsed_time = self.node.now() - self.last_replication
            # We'll set this to true if we replicated to anyone
            replicated = False
            # We need the current term to ensure we don't process responses in later
            # terms.
            term = self.term

            
            if self.state == State.Leader and self.min_replication_interval < elapsed_time:
                # We're a leader, and enough time elapsed
                for n in self.node.other_node_ids():
                    # what entries we should append?
                    n_index = self.next_index[n]
                    self.node.log(f"Replicating from {n_index} to {n}")
                    entries = self.log.from_index(n_index)

                    # if we haven't replicated in the heartbeat interval, we'll send this node an appendEntries message.
                    if len(entries) > 0 or self.heart_beat_interval < elapsed_time:
                        self.node.log(f"Replicating to {n}")
                        replicated = True                        
                        request_body = {
                            'type': 'append_entries',
                            'term': term,
                            'leader_id': self.node.node_id,
                            'entries': entries,
                            'leader_commit': self.commit_index,
                            'prev_log_index': n_index - 1,
                            'prev_log_term': self.log[n_index - 1]['term']
                        }
                        
                        response = self.node.generate_response('append_entries', n, request_body)

                        def callback(result):
                            body = result['body']
                            self.node.log(f"Received {body} from {n}")
                            self.maybe_step_down(body['term'])
                            if self.state == State.Leader:
                                self.reset_stepdown_deadline()
                                if body.get('success'):
                                    self.next_index[n] = max(self.next_index[n], (n_index + len(entries)))
                                    self.match_index[n] = max(self.match_index[n], (n_index + len(entries) - 1))
                                    self.node.log(f'Next index {self.next_index}')
                                    self.advance_commit_index()
                                else:
                                    # We didn't match; back up our next index for this node
                                    # this basically means lets try with previous log entry and see if that succeeds we will keep
                                    # going back to ensure we can replicate all pending logs to the follower 
                                    self.next_index[n] -=1 
                                                    
                        self.node.rpc(response, callback)

                if replicated:
                    self.last_replication = self.node.now()                    


    
    def client_req(self, request):
        with self.lock:
            if self.state != State.Leader:
                if self.leader:
                    body = request['body']
                    self.node.log(f"Not leader, so proxying request to leader")
                    self.node.reply(self.node.generate_response(body['type'], self.leader, body), request)
                else:
                    raise RPCError.temporarily_unavailable("Not leader")
            else: 
                self.log.append([{"term": self.term, "op": request}])
                # self.state_machine, response = self.state_machine.apply(request, self.node)
                # self.node.log(f"@sending_response {response}")
                # self.node.reply(response, request)


    def leader_heart_beat(self):
        time.sleep(random.random()/10)
        with self.lock:
            if self.election_deadline < self.node.now():
                if self.state != State.Leader:
                    self.become_candidate()
                else:
                    self.reset_election_deadline()             
    
    def heart_beat(self):
        with self.lock:
            if self.state == State.Leader and self.stepdown_deadline < self.node.now():
                self.node.log("Stepping down haven't received any acks recently")
                self.become_follower()

    def advance_term(self, term):
        with self.lock:
            if self.term > term:
                raise Exception("Received term is less than current term")
            else:
                self.term = term
                self.voted_for = None

    def become_candidate(self): 
        with self.lock:
            self.state = State.Candidate
            self.advance_term(self.term + 1)
            self.voted_for = self.node.node_id
            self.leader = None            
            self.reset_election_deadline()
            self.reset_stepdown_deadline()
            self.node.log(f"Became candidate for term {self.term}")
            self.request_votes()

    def become_leader(self):
        with self.lock:
            if self.state != State.Candidate:
                raise Exception("Cannot become leader when not candidate")
            self.match_index = {}
            self.next_index = {}
            self.last_replication = time.time() - time.time()
            self.leader = None
            for n in self.node.other_node_ids():
                # we are taking + 1 because first entry in log is empty one
                self.next_index[n] = self.log.size() + 1
                self.match_index[n] = 0

            self.state = State.Leader
            self.reset_stepdown_deadline()
            self.node.log(f"Became leader for term {self.term}")

    def become_follower(self):
        with self.lock:
            self.state = State.Follower
            self.match_index = None
            self.next_index = None
            self.leader = None
            self.reset_election_deadline()
            self.node.log("Became follower")

    def maybe_step_down(self, remote_term):
        with self.lock:
            if self.term < remote_term:
                self.node.log(f"Received term {remote_term} is greater than current term {self.term}. Step down.")
                self.advance_term(remote_term)
                self.become_follower()    


    def request_votes(self):
        with self.lock:
            self.node.log(f"Requesting votes for term {self.term}")
            votes = set()
            votes.add(self.node.node_id)
            term = self.term

            body =  {
                    'type': 'request_vote', 
                    'term': term, 
                    'candidate_id': self.node.node_id,
                    'last_log_index': self.log.size(),
                    'last_log_term': self.log.last()['term']
                }

            def callback(response):
                self.reset_stepdown_deadline()
                with self.lock:
                    body = response['body']
                    if self.state == State.Candidate:
                        if self.term == term and self.term == body['term']:
                            self.node.log(f"vote_granted vote for term {body}")
                            if body['vote_granted']:
                                votes.add(response['src'])
                                self.node.log(f"Have votes: {votes}")
                                if self.majority(len(self.node.node_ids)) <= len(votes):
                                    self.node.log(f"Have majority: {votes}")
                                    self.become_leader()

            self.node.brpc(body, callback)


    def majority(self, n):
        return math.floor(n/2.0) + 1
    

    def mean(self, values):
        values.sort()
        return values[len(values) - self.majority(len(values))]

    def advance_commit_index(self):
        with self.lock:
            if self.state == State.Leader:
                new_commit_index = self.mean(list(self.get_match_index().values()))
                if new_commit_index > self.commit_index and self.term == self.log[new_commit_index]['term']:
                    self.node.log(f"Advancing commit index to {new_commit_index}")
                    self.commit_index = new_commit_index
            self.advance_state_machine()

    def grant_vote(self, request):
        with self.lock:
            body = request['body']
            self.maybe_step_down(body['term'])
            grant = False
            if body['term'] < self.term:
                self.node.log(f"Received term {body['term']} is less than current term {self.term}. Vote not granted.")
            elif self.voted_for:
                self.node.log(f"Already voted for {self.voted_for}. Vote not granted.")
            elif body['last_log_term'] == self.log.last()['term'] and body['last_log_index'] < self.log.size():
                self.log(f"Our logs are both at the term {self.log.last()['term']} but their log size is different so not granting votes")
            else:
                self.node.log(f"Granting vote for term {body['term']}")
                grant = True
                self.voted_for = body['candidate_id']
                

            response = self.node.generate_response('request_vote_res', request['src'], {"term": self.term, "vote_granted": grant})
            self.node.reply(response, request)

    def reset_election_deadline(self):
        with self.lock:
            self.election_deadline = self.node.now() + (self.election_timeout * (random.random() + 1))
    
    def reset_stepdown_deadline(self):
        with self.lock:
            self.stepdown_deadline = self.node.now() + self.election_timeout



Raft().node.main()            


