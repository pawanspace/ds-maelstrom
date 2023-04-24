from thunk import Thunk

class Map():
    def __init__(self, node, id_gen, map = {}):
        self.map = map
        self.node = node
        self.id_gen = id_gen

    def __str__(self):
        return f'{self.map}'
        
    @staticmethod
    def from_json(node, id_gen, json):
        node.log(f"inside:from_json {json}")
        pairs = json if json else []
        m = {}
        for k, id in pairs:
            m[k] = Thunk(node, id, None, True, id_gen)
        return Map(node, id_gen, m)

    #@potential_error the return format can be wrong
    def to_json(self):
        resp = []
        if self.map:        
            for key, value in self.map.items():
                resp.append([key, value.id])
        return resp

    def save(self):
        for thunk in self.map.values():
            thunk.save()
    
    def get(self, key):
        return self.map[key].get_value().copy() if key in self.map else None

    def copy(self):
        return Map(self.node, self.id_gen, self.map.copy() if self.map else {})

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
                l = (ret_map.get(str_key).copy() if ret_map.get(str_key) else [])
                l.append(value)
                ret_map = ret_map.assoc(str_key, l)          
        return [ret_txn, ret_map]
    
    def assoc(self, key, value):
        self.node.log(f"@assoc {key} {value}")
        thunk = Thunk(self.node, self.id_gen.new_id(), value, False, self.id_gen)

        self.node.log(f"@thunk {thunk}")
        merged = {key: thunk}
        #@protential_error respnse format can be different
        for k, v in self.map.items():
            if k == key:
                merged[k] = thunk
            else:
                merged[k] = v

        return Map(self.node, self.id_gen, merged)
    

# m = Map({})
# txn = [['append', 9, 7], ['append', 9, 8], ['r', 9, None]]
# t, m_r = m.transact(txn)
# print(t)
# print(m_r.to_json())
