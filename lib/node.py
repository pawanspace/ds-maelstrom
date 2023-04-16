import sys
import threading
import json
import select

class Node():
    
    def __init__(self):
        self.node_id = None
        self.node_ids = None
        self.next_response_id = 0
        self.lock = threading.RLock()
        self.log_lock = threading.Lock()
        self.handlers = dict()
        self.init_handlers()

    def init_handlers(self):
        self.handlers['init'] = self.handle_init
        self.handlers['echo'] = self.handle_echo

    def handle_init(self, request):
        self.node_id = request['body']['node_id']
        self.log(f'Initialized Node {self.node_id}')
        self.reply(self.generate_response(request, 'init_ok', request['src']), request)

    def handle_echo(self, request):
        response = self.generate_response(request, 'echo_ok', request['src'])
        response['body']['echo'] = request['body']['echo']
        self.reply(response, request)

    def handle_topology(self, request, broadcast):
        broadcast.neighbors = request['body']['topology'][self.node_id]
        self.log(f'My neighbors are {broadcast.neighbors}')
        response = self.generate_response(request, 'topology_ok', request['src'])
        self.reply(response, request)
                
    def handle_broadcast(self, request, broadcast):
        message = request['body']['message']
        # lock this so that we can block handle_read while
        # updating the messages
        with self.lock:
            if message not in broadcast.messages:
                broadcast.messages.add(message)
                
                # send message to all neighbors
                for n in broadcast.neighbors:
                    if n != request['src']:
                        response = self.generate_response(request, 'broadcast', n)
                        response['body']['message'] = message
                        self.send(response)

        # if message is not from neighbor reply with broadcast_ok
        if request['body'].get('msg_id'):
            response = self.generate_response(request, 'broadcast_ok', request['src'])
            self.reply(response, request)


    def handle_read(self, request, broadcast):
        with self.lock:
            response = self.generate_response(request, 'read_ok', request['src'])
            response['body']['messages'] = list(broadcast.messages)
            self.reply(response, request)
           
    def log(self, message):
        with self.log_lock:
            sys.stderr.write(message)
            sys.stderr.flush()

    def generate_response(self, request, response_type, dest):
        response = dict()
        response['src'] = self.node_id
        response['dest'] = dest
        body = dict()
        body['type'] = response_type
        response['body'] = body
        return response

    def send(self, response):
        with self.lock:        
            self.log(f'Sending response: {response}')
            json.dump(response, sys.stdout)
            sys.stdout.write('\n')
            sys.stdout.flush()

    def reply(self, response, request):
        response['body']['in_reply_to'] = request['body']['msg_id']
        self.send(response)

    def parse_message(self, incoming):
        return json.loads(incoming)

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
                handler = self.handlers.get(request_type)
                if handler:
                    handler(request)
                else:
                    raise Exception(f'Unable to find handler for request type: {request_type}')
