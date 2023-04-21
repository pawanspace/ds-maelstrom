import sys
import threading
import json
import select
import time
from promise import Promise


class RPCError(BaseException):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(self.message)
    
    @staticmethod 
    def timeout(msg): return RPCError(0, msg)
    @staticmethod 
    def not_supported(msg): return RPCError(10, msg)
    @staticmethod 
    def temporarily_unavailable(msg): return RPCError(11, msg)
    @staticmethod 
    def malformed_request(msg): return RPCError(12, msg)
    @staticmethod 
    def crash(msg): return RPCError(13, msg)
    @staticmethod 
    def abort(msg): return RPCError(14, msg)
    @staticmethod 
    def key_does_not_exist(msg): return RPCError( 20, msg)
    @staticmethod 
    def precondition_failed(msg): return RPCError( 22, msg)
    @staticmethod 
    def txn_conflict(msg): return RPCError(30, msg)

    def to_json(self):
        return {'type': "error", 'code': self.code, 'text': self.message}


class Node():
    
    def __init__(self):
        self.node_id = None
        self.node_ids = None
        self.next_response_id = 0
        self.lock = threading.RLock()
        self.log_lock = threading.Lock()
        self.handlers = dict()
        self.callbacks = dict()
        self.periodic_tasks = dict() # key is duration
        self.init_handlers()

    def every(self, task, delay):
        self.periodic_tasks[delay] = task
        
    def start_periodic_tasks(self):
        for delay, task in self.periodic_tasks.items():
            t = threading.Thread(target=self.periodic_task_runner, args=(task, delay))
            t.start()

    def periodic_task_runner(self, task, delay):
        while True:
            task()
            time.sleep(delay)

        
    def init_handlers(self):
        self.handlers['init'] = self.handle_init
        self.handlers['echo'] = self.handle_echo

    def handle_add(self, request, server):
        with server.lock:
            server.messages.add(request['body']['element'])
        self.reply(self.generate_response('add_ok', request['src']), request)
        
    def handle_init(self, request):
        self.node_id = request['body']['node_id']
        self.node_ids = request['body']['node_ids']
        self.log(f'Initialized Node {self.node_id}')
        self.reply(self.generate_response('init_ok', request['src']), request)
        self.start_periodic_tasks()

    def handle_echo(self, request):
        response = self.generate_response('echo_ok', request['src'])
        response['body']['echo'] = request['body']['echo']
        self.reply(response, request)

    def handle_topology(self, request, broadcast):
        broadcast.neighbors = request['body']['topology'][self.node_id]
        self.log(f'My neighbors are {broadcast.neighbors}')
        response = self.generate_response('topology_ok', request['src'])
        self.reply(response, request)
                
    def handle_broadcast(self, request, broadcast):
        response = self.generate_response('broadcast_ok', request['src'])
        self.reply(response, request)
        
        message = request['body']['message']
        new_message = False
        # lock this so that we can block handle_read while
        # updating the messages
        with self.lock:
            if message not in broadcast.messages:
                broadcast.messages.add(message)
                new_message = True

        if (new_message):
            neighbors = broadcast.neighbors.copy()            
            self.log(f'Sending message to neighbors: {neighbors}')
            neighbors.remove(request['src']) if request['src'] in neighbors else None
            # send message to all neighbors
            while neighbors:
                for n in neighbors:
                    response = self.generate_response('broadcast', n)
                    response['body']['message'] = message
                    callback = lambda resp: neighbors.remove(n) if (resp['body']['type'] == 'broadcast_ok' and n in neighbors) else None
                    self.rpc(response, callback)
                time.sleep(1)
        self.log(f'Done with message: {message}')


    def handle_read(self, request, broadcast, message_key = 'messages', lock = None):
        if lock == None:
            lock = self.lock
            
        with lock:
            response = self.generate_response('read_ok', request['src'])
            response['body'][message_key] = list(broadcast.messages)
            self.reply(response, request)

    def handle_replicate(self, request, server):
        with server.lock:
            server.messages.union(request['body']['value'])
            
    def log(self, message):
        with self.log_lock:
            sys.stderr.write(message)
            sys.stderr.flush()

    def generate_response(self, response_type, dest):
        response = dict()
        response['src'] = self.node_id
        response['dest'] = dest
        body = dict()
        body['type'] = response_type
        response['body'] = body
        return response

    
    def send(self, response):
        with self.lock:        
            self.log(f'Sending response: {type(response)}')            
            json.dump(response, sys.stdout)
            sys.stdout.write('\n')
            sys.stdout.flush()

    def reply(self, response, request):
        response['body']['in_reply_to'] = request['body']['msg_id']
        self.send(response)

    def parse_message(self, incoming):
        return json.loads(incoming)

    def rpc(self, response, handler):
        with self.lock:
            self.next_response_id += 1
            msg_id = self.next_response_id
            self.callbacks[msg_id] = handler
            response['body']['msg_id'] = msg_id
            self.send(response)

    def add_msg_id(self, response):
        with self.lock:
            self.next_response_id += 1
            msg_id = self.next_response_id
            response['body']['msg_id'] = msg_id            

    def sync_rpc(self, dest, body, action):
        response = self.generate_response(action, dest)
        response['body'] = response['body'] | body
        p = Promise()
        self.rpc(response, lambda resp: p.resolve(resp))
        return p.await_promise()

    def handler_exec(self, handler, request):
        try:
            handler(request)
        except RPCError as e:
            response = self.generate_response('error', request['src'])
            response['body'] = e.to_json()
            self.reply(response, request)
        except Exception as e:
            self.log(f'Exception handling {request}:\n{getattr(e, "message", repr(e))}')     
            response = self.generate_response('error', request['src'])
            response['body'] = RPCError.crash(getattr(e, 'message', repr(e))).to_json()
            self.reply(response, request)
        

    def main(self):
        while True:
            """Handles a message from stdin, if one is currently available."""
            if sys.stdin not in select.select([sys.stdin], [], [], 0)[0]:
                continue

            line = sys.stdin.readline()
            if not line:
                continue

            request = self.parse_message(line)
            self.log(f'Received message: {request}')

            with self.lock:                
                request_type = request['body']['type']
                handler = None

                # check if its response of a broadcast from this node
                if request['body'].get('in_reply_to'):
                    in_reply_to = request['body'].get('in_reply_to')
                    handler = self.callbacks.pop(in_reply_to)
                    self.log(f'Handling callback for {in_reply_to}')
                else:
                    handler = self.handlers.get(request_type)
                    
                if handler:
                    t = threading.Thread(target=handler, args=(request,))
                    t.start()
                else:
                    raise Exception(f'Unable to find handler for request type: {request_type}')
