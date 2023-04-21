#!/usr/bin/env python

import threading
from node import Node
from immutable_map import Map

class State():
    KEY = 'root'
    def __init__(self, node):
        self.node = node

    def transact(self, txn):
        body = {'key': State.KEY} 
        resp = self.node.sync_rpc('lin-kv', body, 'read')
        self.node.log(f'#####ReadData {State.KEY}: {resp}')                         
        map = Map(resp['body'].get('value'))
        txn_resp, map_resp = map.transact(txn)
        
        body = body | {'from': map.to_json(), 'to': map_resp.to_json(), 'create_if_not_exists': 'true'}
        self.node.log(f'#####PCPCPCPC sending response with body: {body}')
        resp = self.node.sync_rpc('lin-kv', body, 'cas')       

        if resp['body'] ['type'] != 'cas_ok':
            raise Exception(f'CAS failed for {State.KEY}')
                
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

