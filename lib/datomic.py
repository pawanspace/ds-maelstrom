#!/usr/bin/env python

import threading
from node import Node

class State():
    def __init__(self, node):
        self.node = node

    def transact(self, txn):
        txn_resp = []
        for t in txn:
            op, key, value = t
            if op == 'r':      
                body = {'key': key} 
                resp = self.node.sync_rpc('lin-kv', body, 'read')
                self.node.log(f'Read {key}: {resp}')                         
                list = resp['body'].get('value')
                txn_resp.append([op, key, list])
            elif op == 'append':
                txn_resp.append([op, key, value])
                body = {'key': key}              
                resp = self.node.sync_rpc('lin-kv', body, 'read')
                current = resp['body'].get('value')

                new = current.copy() if current else []
                new.append(value)

                body = body | {'from': current, 'to': new,'create_if_not_exists': 'true'}
                resp = self.node.sync_rpc('lin-kv', body, 'cas')        
                if resp['body'] ['type'] != 'cas_ok':
                    raise Exception(f'CAS failed for {key}')
                
        return txn_resp

class Transactor():    
    def __init__(self):
        self.node = Node()
        self.lock = threading.Lock()
        self.state = State(self.node)
        self.node.handlers['txn'] = self.transact

    def transact(self, request):
        txn = request['body']['txn']
        self.node.log(f'Handling transaction: {txn}')
        txn_resp = []
        with self.lock:
            txn_resp = self.state.transact(txn)
        
        response = self.node.generate_response('txn_ok', request['src'])
        self.node.add_msg_id(response)
        response['body']['txn'] = txn_resp        
        self.node.reply(response, request)

Transactor().node.main()        

