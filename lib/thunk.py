from id_gen import IDGen
from node import RPCError
import time

class Thunk():
    def __init__(self, node, id, value, saved, id_gen):
        self.node = node
        self.id = id
        self.value = value
        self.saved = saved
        self.id_gen = id_gen


    def __str__(self):
        return f'id: {self.id}, value: {self.value}, saved: {self.saved}'
        
    def get_id(self):
        return self.id if self.id else self.id_gen.new_id
        
    def get_value(self):
        if self.value:
            return self.value
        else:           
            body = {'key': self.id}
            resp = self.node.sync_rpc('lww-kv', body, 'read')['body']               
            while True:
                if resp['type'] == 'read_ok':
                    self.value = resp['value']
                    return self.value
                else:
                    time.sleep(0.01)   
            
    def save(self):
        while not self.saved:
            body = {'key': self.id, 'value': self.value}
            resp = self.node.sync_rpc('lww-kv', body, 'write')
            if resp['body']['type'] == 'write_ok':
                self.saved = True
            else:
                raise RPCError.abort(f'Unable to save thunk with id: {self.id}')
            
