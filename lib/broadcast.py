#!/usr/bin/env python

from node import Node

class Broadcast():
    def __init__(self):
        self.node = Node()
        self.neighbors = []
        self.messages = set()
        self.node.handlers['topology'] = lambda request: self.node.handle_topology(request, self)
        self.node.handlers['broadcast'] = lambda request: self.node.handle_broadcast(request, self)
        self.node.handlers['read'] = lambda request: self.node.handle_read(request, self)
        

Broadcast().node.main()
