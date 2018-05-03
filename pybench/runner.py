from concurrent.futures import ProcessPoolExecutor, wait
from numpy import median, mean
class TaskObject(object):

    def do_stuff(self, worker_id):
        """ Returns mean, median, min, max as a dict"""
        return dict(total=0, mean_v=0, median_v=0, min_v=0, max_v=0, values=[])
class TaskRunner(object):

    def __init__(self, num_workers, task_objects):
        self._num_workers = num_workers
        self._task_objects=task_objects

    def run_tasks(self):
        futures = []
        with ProcessPoolExecutor(max_workers=self._num_workers) as executor:
            print("Creating {} workers".format(self._num_workers))
            for i, task in zip(range(self._num_workers), self._task_objects):
                futures.append(executor.submit(task.do_stuff, i))
            print("Workers created, waiting for them to be completed")
            wait(futures)
            print("Workers ended!")
        self._results = [f.result() for f in futures]

    def get_header(self):
        return ("N,mean,median,min,max")

    def get_result_summary(self, field="total"):
        value_list = [x[field] for x in self._results]
        return("{},{},{},{},{}".format(self._num_workers,mean(value_list),
            median(value_list),min(value_list),max(value_list)))

