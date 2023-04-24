import threading

class IDGen():
    def __init__(self, node):
        self.id = -1
        self.node = node
        self.mutex = threading.Lock()


    def new_id(self):
        with self.mutex:
            self.id += 1
            return f'{self.node.node_id}-{self.id}'
        
