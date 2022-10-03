from datetime import datetime as dt
import time
import datetime


class Logger:
    def __init__(self, filename='report'):
        self.filename = filename
        self.start_import = datetime.datetime.now()
        self.start_block = time.time()
        self.start_block_time = dt.now()

    def log(self):
        with open(self.filename + '.csv', 'a') as f:
            elapsed_time_block = dt.fromtimestamp(time.time()-self.start_block)
            timedelta = int((datetime.datetime.now() -
                            self.start_import).total_seconds())
            minutes = 0 if timedelta < 60 else timedelta // 60
            seconds = timedelta - (minutes*60)
            minutes_str = '0' + str(minutes) if minutes < 10 else minutes
            seconds_str = '0' + str(seconds) if seconds < 10 else seconds
            msg = f'{dt.now().isoformat()};{minutes_str}:{seconds_str};{elapsed_time_block.strftime("%M:%S")}\n'
            f.write(msg)
            self.start_block = time.time()
            f.flush()
