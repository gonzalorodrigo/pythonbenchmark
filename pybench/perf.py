
import datetime

class TM(object):

    def __init__(self, msg="", w=None):
        self._start_time = None
        self._stop_time = None
        self._msg=msg
        self._worker=w

    def start(self):
        self._stop_time = None
        self._start_time = datetime.datetime.now()
        return self

    def stop(self, report=True):
        self._stop_time = datetime.datetime.now()
        seconds = self.get_time_s()
        if report:
            worker_s =""
            if self._msg:
                if self._worker is not None:
                    worker_s = "W({}): ".format(self._worker)
                print("{}Op({}) took {}s ".format(worker_s, self._msg, seconds))
        return seconds

    def get_time_s(self):
        if self._start_time is None or self._stop_time is None:
            return False
        delta = (self._stop_time-self._start_time)
        return float(delta.seconds)+float(delta.microseconds)/1000000

