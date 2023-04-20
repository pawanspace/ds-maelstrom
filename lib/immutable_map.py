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

    def transact(self, txn):
        ret_txn = []
        ret_map = Map({})
        for t in txn:
            op, key, value = t
            if op == 'r':          
                ret_txn.append([op, key, ret_map.get(key)])
            elif op == 'append':
                ret_txn.append([op, key, value])
                list = (ret_map.get(key) or [])
                list.append(value)
                ret_map = ret_map.assoc(key, list)                    
        return [ret_txn, ret_map]


    def assoc(self, key, value):
        updated = {key: value} | self.map
        return Map(updated)