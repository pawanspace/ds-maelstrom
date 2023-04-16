#!/usr/bin/env python

import threading
from node import Node


class Counter():
    def __init__(self, counters = dict()):
        self.counters = counters

    def from_json(self, json):
        return Counter(json)

    def to_json(self):
        return self.counters

    def read(self):
        return sum(self.counters.values())

    def _none_or_max(self, a, b):        
        if  a is None:
            return b
        if b is None:
            return a
        return max(a, b)
    
    def merge(self, other):
        # get max in case key exists in both Counters
        all_keys = self.counters.keys() | other.counters.keys()                
        return Counter({k: self._none_or_max(self.counters.get(k), other.counters.get(k)) for k in all_keys})

    def add(self, node_id, delta):
        local = self.counters.copy()
        local[node_id] = local[node_id] + delta if local.get(node_id) else delta
        return Counter(local)
    

class CounterServer():
    def __init__(self):
        self.node = Node()
        self.lock = threading.Lock()
        self.crdt = Counter()            
        self.node.handlers['add'] = lambda request: self.add(request)
        self.node.handlers['read'] = lambda request: self.read(request)
        self.node.handlers['replicate'] = lambda request: self.merge(request)
        self.node.every(self.replicate_counters, 5)

        
    def replicate_counters(self):
        self.node.log(f'Replicating counters: {self.crdt.to_json()}')
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
            self.node.log(f'Using  {request["body"]["value"]} Replicated counters: {self.crdt.read()}')

    def add(self, request):
        with self.lock:
            self.crdt = self.crdt.add(self.node.node_id, request['body']['delta'])
        self.node.reply(self.node.generate_response('add_ok', request['src']), request)
                    

CounterServer().node.main()
