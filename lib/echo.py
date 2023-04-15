#!/usr/bin/env python
import sys
import json
from types import SimpleNamespace
import select

class EchoServer():

    def __init__(self):
        self.node_id = None
        self.response_id = 0
        
    def main(self):
        while True:
            """Handles a message from stdin, if one is currently available."""
            if sys.stdin not in select.select([sys.stdin], [], [], 0)[0]:
                continue
            
            line = sys.stdin.readline()
            if not line:
                continue
            
            request = json.loads(line, object_hook=lambda d: SimpleNamespace(**d))
            print(f'Received {request}', file=sys.stderr)
            self.handle_message(request)            
            line = sys.stdin
    
    def handle_message(self, request):
        if (request.body.type == 'init'):
            self.node_id = request.body.node_id
            print(f'Initialized Node {self.node_id}', file=sys.stderr)
            self.send(self.generate_response(request, 'init_ok'))
        elif (request.body.type == 'echo'):
            response = self.generate_response(request, 'echo_ok')
            response['body']['echo'] = request.body.echo           
            self.send(response)                                              

    def generate_response(self, request, response_type):
        response = dict()
        response['src'] = request.dest
        response['dest'] = request.src
        body = dict()
        self.response_id += 1
        body['msg_id'] = self.response_id
        body['in_reply_to'] = request.body.msg_id
        body['type'] = response_type
        response['body'] = body
        return response
        
    def send(self, response):
        json.dump(response, sys.stdout)
        sys.stdout.write('\n')
        sys.stdout.flush()
        
        


EchoServer().main()    
    
            
