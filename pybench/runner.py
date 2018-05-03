from concurrent.futures import ProcessPoolExecutor, wait
from numpy import median, mean
class TaskObject(object):

    def do_stuff(self, worker_id, debug=False):
        """ Returns mean, median, min, max as a dict"""
        return dict(total=0, mean_v=0, median_v=0, min_v=0, max_v=0, values=[])
class TaskRunner(object):

    def __init__(self, num_workers, task_objects):
        self._num_workers = num_workers
        self._task_objects=task_objects

    def run_tasks(self, debug=False):
        futures = []
        with ProcessPoolExecutor(max_workers=self._num_workers) as executor:
            if debug:
                print("Creating {} workers".format(self._num_workers))
            for i, task in zip(range(self._num_workers), self._task_objects):
                futures.append(executor.submit(task.do_stuff, i, debug=debug))
            if debug:
                print("Workers created, waiting for them to be completed")
            wait(futures)
            if debug:
                print("Workers ended!")
        self._results = [f.result() for f in futures]

    def get_header(self):
        return ("N,mean,median,min,max")

    def get_result_summary(self, field="total"):
        value_list = [x[field] for x in self._results]
        return("{},{},{},{},{}".format(self._num_workers,mean(value_list),
            median(value_list),min(value_list),max(value_list)))

def run_experiment(name, task_class, min_workers=1, max_workers=16, step=1, 
                   debug=False, *args, **kwargs):
    max_workers+=1
    print("{} exp STARTED. Workers: {}".format(name, list(range(min_workers,
        max_workers, step))))
    task_runners = []
    for num_workers in range (min_workers, max_workers, step):
        tasks = [task_class(*args, **kwargs) for x in range(num_workers)]
        tr = TaskRunner(num_workers, tasks)
        task_runners.append(tr)
        if debug:
            print("Running exp({})".format(num_workers))
        tr.run_tasks(debug=debug)
        if debug:
            print("Exp({}) run: {}".format(num_workers, tr.get_result_summary()))

    print("Task runtime per worker")
    first=True
    for tr in task_runners:
        if first:
            print(tr.get_header())
            first=False
        print(tr.get_result_summary())
    print("{} exp COMPLETED".format(name))


