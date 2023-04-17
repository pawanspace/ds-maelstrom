#!/usr/bin/env python

import threading
from node import Node


class State():
    def __init__(self):
        self.state = dict()

    def transact(self, txn):
        txn_resp = []
        for t in txn:
            op, key, value = t
            if op == 'r':
                txn_resp = [op, key, self.state[key] if self.state.get(key) else []]
            elif op == 'append':
                txn_resp.append(op)
                values = self.state[key].copy() if self.state.get(key) else []
                values.append(value)
                self.state[key] = values
        return txn_resp

class Transactor():    
    def __init__(self):
        self.node = Node()
        self.lock = threading.Lock()
        self.state = State()
        self.node.handlers['txn'] = self.transact

    def transact(self, request):
        txn = request['body']['txn']
        self.node.log(f'Handling transaction: {txn}')
        txn_resp = []
        with self.lock:
            txn_response = self.state.transact(txn)
        
        response = self.node.generate_response('txn_ok', request['src'])
        response['body']['txn'] = txn_resp
        self.node.reply(response, request)

Transactor().node.main()        

