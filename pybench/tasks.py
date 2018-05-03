from pybench.runner import TaskObject
from pybench.perf import TM
from math import sqrt




class CPUTask(TaskObject):

    def __init__(self, reps=10000):
        self._reps=reps

    def do_stuff(self, worker_id):
        print ("Worker {} starts".format(worker_id))
        t = TM("Worker {} task".format(worker_id)).start()
        for i in range(self._reps):
            self._n=sqrt(2**2*2**2+2**2*2**2+2)
        s=t.stop()
        print ("Worker {} ends".format(worker_id))
        return dict(total=s, mean_v=s, median_v=s, min_v=s, max_v=s, values=[s])

