import threading

class Promise():
    WAITING = {}
    TIMEOUT = 10

    def __init__(self):
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.value = Promise.WAITING
        
    def await_promise(self):
        if self.value != Promise.WAITING:
            return self.value
        
        with self.condition:
            self.condition.wait(Promise.TIMEOUT)

        if self.value != Promise.WAITING:
            return self.value
        else:
            raise Exception("Promise timed out")

    def resolve(self, value):
        self.value = value
        with self.condition:
            self.condition.notify()
        return self
    















