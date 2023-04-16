#!/usr/bin/env python

import threading
from node import Node


class GSet():
    def __init__(self, messages = set()):
        self.messages = messages

    def from_json(self, json_array):
        return GSet(set(json_array))

    def to_json(self):
        return list(self.messages)

    def read(self):
        return list(self.messages)

    def merge(self, other):
        return GSet(self.messages.union(other.messages))

    def add(self, element):
        local = self.messages.copy()
        local.add(element)
        return GSet(local)
    

class GSetServer():
    def __init__(self):
        self.node = Node()
        self.lock = threading.Lock()
        self.crdt = GSet()            
        self.node.handlers['add'] = lambda request: self.add(request)
        self.node.handlers['read'] = lambda request: self.read(request)
        self.node.handlers['replicate'] = lambda request: self.merge(request)
        self.node.every(self.replicate_messages, 5)

        
    def replicate_messages(self):
        self.node.log(f'Replicating messages: {self.crdt.to_json()}')
        for node_id in self.node.node_ids:
            if node_id != self.node.node_id:
                response = self.node.generate_response('replicate', node_id)
                response['body']['value'] = self.crdt.to_json()
                self.node.send(response)

    def read(self, request):
        with self.lock:
            response = self.node.generate_response('read_ok', request['src'])
            response['body']['value'] = self.crdt.read()
            self.node.reply(response, request)

    def merge(self, request):
        with self.lock:
            other = self.crdt.from_json(request['body']['value'])
            self.crdt = self.crdt.merge(other)
            self.node.log(f'Using  {request["body"]["value"]} Replicated messages: {self.crdt.read()}')

    def add(self, request):
        with self.lock:
            self.crdt = self.crdt.add(request['body']['element'])
        self.node.reply(self.node.generate_response('add_ok', request['src']), request)
                    

GSetServer().node.main()
