#!/usr/bin/env python

import threading
from node import Node
from node import RPCError
from id_gen import IDGen
from immutable_map import Map

class State():
    KEY = 'root'
    def __init__(self, node, id_gen):
        self.node = node
        self.id_gen = id_gen

    def transact(self, txn):
        body = {'key': State.KEY} 
        resp = self.node.sync_rpc('lin-kv', body, 'read')
        self.node.log(f'#####ReadData {State.KEY}: {resp}')
        saved = True if resp['body'].get('value') else False
        has_existing_value = True if resp['body'].get('value') else False
        map_id = resp['body']['value'] if has_existing_value else self.id_gen.new_id()
        map_value = None if has_existing_value else {}
        map = Map(self.node, self.id_gen, map_id, saved, map_value)
        
        self.node.log(f'#####from_json {map.map}')                         
        
        txn_resp, map_resp = map.transact(txn)
        
        self.node.log(f"@map_resp {map_resp}")

        if map.id != map_resp.id:
            #Save all thunks
            map_resp.save()
            body = body | {'from': map.id, 'to': map_resp.id, 'create_if_not_exists': 'true'}
            self.node.log(f'#####PCPCPCPC sending response with body: {body}')
            resp = self.node.sync_rpc('lin-kv', body, 'cas')       

            if resp['body']['type'] != 'cas_ok':
                self.node.log(f"@error for cas {resp}")
                raise RPCError.txn_conflict(f'CAS failed for {State.KEY}')
                
        return txn_resp

class Transactor():    
    def __init__(self):
        self.node = Node()
        self.lock = threading.Lock()
        self.state = State(self.node, IDGen(self.node))        
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

