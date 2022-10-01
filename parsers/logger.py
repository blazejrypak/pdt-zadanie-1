from datetime import datetime as dt
import time
import datetime


class Logger:
    def __init__(self, filename='report'):
        self.filename = filename
        self.start_import = time.time()
        self.start_block = time.time()
        self.start_block_time = dt.now()
    
    def log(self):
        with open(self.filename + '.txt', 'a') as f:
            elapsed_time_block = dt.fromtimestamp(time.time()-self.start_block)
            msg = f'{dt.now().isoformat()};{dt.fromtimestamp(time.time()-self.start_import).strftime("%M:%S")};{elapsed_time_block.strftime("%M:%S")}\n'
            f.write(msg)
            self.start_block = time.time()
            f.flush()