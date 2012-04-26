import threading

class PoolThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        self.semaphore = kwargs.pop('semaphore')
        threading.Thread.__init__(self, *args, **kwargs)
    
    def run(self):
        self.semaphore.acquire()
        threading.Thread.run(self)
        self.semaphore.release()

class Pool(object): #a threading pool
    def __init__(self, size):
        self.size = size
        self.pool_semaphore = threading.Semaphore(size)
        self.threads = list()
    
    def apply_async(self, func, args=(), kwargs={}):
        self.wait_available()
        thread = PoolThread(target=func, args=args, kwargs=kwargs, semaphore=self.pool_semaphore)
        thread.start()
        self.threads.append(thread)
    
    def wait_available(self):
        self.pool_semaphore.acquire()
        self.pool_semaphore.release()
    
    def join(self):
        for thread in self.threads:
            thread.join()

