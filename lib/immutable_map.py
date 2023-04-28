from thunk import Thunk
from node import RPCError
import time

SVC = 'lww-kv'
class Map():
    def __init__(self, node, id_gen, id, saved, map = None):
        self.map = map
        self.node = node
        self.id_gen = id_gen
        self.id = id
        self.saved = saved

    def __str__(self):
        return f'{self.map}'
        
    def from_json(self, json):
        self.node.log(f"inside:from_json {json}")
        pairs = json if json else []
        m = {}
        for k, id in pairs:
            m[k] = Thunk(self.node, id, None, True, self.id_gen)
        return m

    #@potential_error the return format can be wrong
    def to_json(self):
        resp = []
        if self.map:        
            for key, value in self.map.items():
                resp.append([key, value.get_id()])
        return resp

    def save(self):
        for thunk in self.map.values():
            thunk.save()
            self.save_self()
    
    def get(self, key):
        self.map = self.get_map()
        if self.map and self.map.get(key) and self.map.get(key).get_value():
            return self.map.get(key).value.copy()
        else:
            return None

    def copy(self):
        return Map(self.node, self.id_gen,  self.id, False, self.map.copy() if self.map else None)

    def get_map(self):
        body = {'key': self.id}
        while not self.map:
            resp = self.node.sync_rpc(SVC, body, 'read')
            if resp['body']['type'] == 'read_ok':
                self.map = self.from_json(resp['body'].get('value'))
                continue
            else:
                time.sleep(0.01)
                continue

        return self.map
            
    def save_self(self):
        self.node.log(f"@saving_self with id: {self.id}")
        while not self.saved:
            body = {'key': self.id, 'value': self.to_json()}
            resp = self.node.sync_rpc(SVC, body, 'write')
            if resp['body']['type'] == 'write_ok':
                self.saved = True
            else:
                raise RPCError.abort(f'Unable to save thunk with id: {self.id}')

    
    def transact(self, txn):
        ret_txn = []
        ret_map = self.copy()
        for t in txn:
            op, key, value = t
            str_key = str(key)
            if op == 'r':          
                ret_txn.append([op, key, ret_map.get(str_key)])
            elif op == 'append':
                ret_txn.append(t)
                list = []
                if ret_map.get(str_key):
                    list = ret_map.get(str_key)

                list.append(value)
                ret_map = ret_map.assoc(str_key, list)          
        return [ret_txn, ret_map]
    

    def assoc(self, key, value):
        self.node.log(f"@assoc {key} {value}")
        thunk = Thunk(self.node, self.id_gen.new_id(), value, False, self.id_gen)

        self.node.log(f"@thunk {thunk}")
       # merged = {key: thunk}
        # #@protential_error respnse format can be different
        # for k, v in self.map.items():
        #     if k == key:
        #         existing_value = v.get_value()
        #         self.node.log(f'@thunk_merging to {thunk.id} with value {thunk.value} from {v.id} with value {existing_value}')
        #         thunk.value = list(set((existing_value if existing_value else []) + thunk.value))
        #         self.node.log(f'@thunk_merging_result {thunk.value}')  
        #        merged[k] = thunk
            # else:
            #     merged[k] = v

        merged = self.map.copy()
        merged[key] = thunk
        return Map(self.node, self.id_gen, self.id_gen.new_id(), False, merged)
    

# m = Map({})
# txn = [['append', 9, 7], ['append', 9, 8], ['r', 9, None]]
# t, m_r = m.transact(txn)
# print(t)
# print(m_r.to_json())
