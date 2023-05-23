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
        return self.entries[index]
    
    def append(self, entries):
        self.entries.extend(entries)

    def last(self):
        return self.entries[-1]

    def size(self):
        return len(self.entries)

class State(Enum):
    Follower = 1,
    Candidate = 2,
    Leader = 3

class Raft():
    def __init__(self):
        self.node = Node()
        self.lock = threading.RLock()
        self.state_machine = Map()
        self.state = State.Follower
        self.election_timeout = 2
        self.election_deadline = self.node.now()
        self.stepdown_deadline = self.node.now()
        self.term = 0
        self.log = Log(self.node)
        self.voted_for = None

        self.node.handlers['read'] = lambda request: self.client_req(request)
        self.node.handlers['write'] = lambda request: self.client_req(request)
        self.node.handlers['cas'] = lambda request: self.client_req(request)
        self.node.handlers['request_vote'] = lambda request: self.grant_vote(request)

        self.node.every(lambda: self.leader_heart_beat(), 0.1)
        self.node.every(lambda: self.heart_beat(), 1)
        

    def client_req(self, request):
        with self.lock:
            self.state_machine, response = self.state_machine.apply(request, self.node)
            self.node.reply(response, request)

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
            self.reset_election_deadline()
            self.reset_stepdown_deadline()
            self.node.log(f"Became candidate for term {self.term}")
            self.request_votes()

    def become_leader(self):
        with self.lock:
            if self.state != State.Candidate:
                raise Exception("Cannot become leader when not candidate")
            
            self.state = State.Leader
            self.reset_stepdown_deadline()
            self.node.log(f"Became leader for term {self.term}")

    def become_follower(self):
        with self.lock:
            self.state = State.Follower
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
                            if body['vote_granted']:
                                votes.add(response['src'])
                                self.node.log(f"Have votes: {votes}")
                                if self.majority(len(self.node.node_ids)) <= len(votes):
                                    self.node.log(f"Have majority: {votes}")
                                    self.become_leader()

            self.node.brpc(body, callback)


    def majority(self, n):
        return math.floor(n/2.0) + 1


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


