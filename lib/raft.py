#!/usr/bin/env python

import threading
from node import Node
from node import RPCError

class Map():
    def __init__(self, data = dict()):
        self.data = data

    def apply(self, request, node):       
        op = request['body']
        key = op['key']
        match op['type']:
            case "read":
                return [self, self.generate_response(node, 'read_ok', request['src'], {"type": "read_ok", "value": self.data[key]})] if self.data.get(key) else [self, RPCError.key_does_not_exist('not found').to_json()]
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
            
            


class Raft():
    def __init__(self):
        self.node = Node()
        self.lock = threading.Lock()
        self.state_machine = Map()

        self.node.handlers['read'] = lambda request: self.client_req(request)
        self.node.handlers['write'] = lambda request: self.client_req(request)
        self.node.handlers['cas'] = lambda request: self.client_req(request)
        

    def client_req(self, request):
        with self.lock:
            self.state_machine, response = self.state_machine.apply(request, self.node)
            self.node.reply(response, request)


Raft().node.main()            


