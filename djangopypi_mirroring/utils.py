import threading
import time

from django.db import connection


class PoolThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        self.semaphore = kwargs.pop('semaphore')
        self.retries_left = kwargs.pop('retries')
        self.retry_wait = kwargs.pop('retry_wait')
        threading.Thread.__init__(self, *args, **kwargs)
    
    def run(self):
        success = False
        self.semaphore.acquire()
        try:
            while self.retries_left and not success:
                try:
                    threading.Thread.run(self)
                except:
                    if not self.retries_left:
                        raise
                    self.retries_left -= 1
                    time.sleep(self.retry_wait)
                else:
                    success = True
        finally:
            try:
                connection.close()
            except:
                pass
            self.semaphore.release()

class Pool(object): #a threading pool
    def __init__(self, size, retries=3, retry_wait=3):
        self.size = size
        self.retries = retries
        self.retry_wait = retry_wait
        self.pool_semaphore = threading.Semaphore(size)
        self.threads = list()
    
    def apply_async(self, func, args=(), kwargs={}):
        self.wait_available()
        thread = PoolThread(target=func, args=args, kwargs=kwargs,
                            semaphore=self.pool_semaphore,
                            retries=self.retries,
                            retry_wait=self.retry_wait,)
        thread.start()
        self.threads.append(thread)
    
    def wait_available(self):
        self.pool_semaphore.acquire()
        self.pool_semaphore.release()
    
    def join(self):
        for thread in self.threads:
            thread.join()

