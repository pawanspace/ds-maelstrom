class Map():
    def __init__(self, map):
        self.map = map

    def from_json(self, json_map):
        if json_map:
            return Map(json_map)    
        else:
            return Map({})
        
    def to_json(self):
        return self.map
    
    def get(self, key):
        return self.map[key].copy() if key in self.map else None

    def copy(self):
        return Map(self.map.copy() if self.map else {})

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
                ret_map = ret_map.assoc(str(key), l)          
        return [ret_txn, ret_map]
    
    def assoc(self, key, value):    
        merged = {key: value} 
        for k, v in self.map.items():
            if k in merged:
                merged[k] = list(set(merged[k] + v))
            else:
                merged[k] = v
        return Map(merged)
    

# m = Map({})
# txn = [['append', 9, 7], ['append', 9, 8], ['r', 9, None]]
# t, m_r = m.transact(txn)
# print(t)
# print(m_r.to_json())