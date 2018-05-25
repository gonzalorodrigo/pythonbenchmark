from concurrent.futures import ProcessPoolExecutor, wait
from statistics import median, mean
from pybench.perf import TM
class TaskObject(object):


    def do_stuff(self, worker_id, debug=False):
        msg=""
        if debug:
            print ("Worker {} starts".format(worker_id))
            msg="Worker {} task".format(worker_id)
        t = TM(msg).start()
        self.do_op(worker_id, debug=debug)
        s=t.stop()
        if debug:
            print ("Worker {} ends".format(worker_id))
        return dict(total=s, mean_v=s, median_v=s, min_v=s, max_v=s, values=[s])
    
    def do_op(self, worker_id, debug=False):
        pass
class TaskRunner(object):

    def __init__(self, num_workers, task_objects):
        self._num_workers = num_workers
        self._task_objects=task_objects

    def run_tasks(self, debug=False):
        futures = []
        tm_str =""
        if debug:
            tm_str = "Total exp runtime"
        tm= TM(tm_str).start()

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
        self._total_time = tm.stop()
        self._results = [f.result() for f in futures]

    def get_header(self):
        return ("N,total,mean,median,min,max")

    def get_result_summary(self, field="total"):
        value_list = [x[field] for x in self._results]
        return("{},{},{},{},{},{}".format(self._num_workers, self._total_time,
            mean(value_list), median(value_list),min(value_list),
            max(value_list)))

def run_experiment(name, task_class, min_workers=0, max_workers=16, step=1, 
                   debug=False, *args, **kwargs):
    max_workers+=1
    print("{} exp STARTED. Workers: {}".format(name, list(range(min_workers,
        max_workers, step))[1:]))
    print("ARGS", args, kwargs)
    task_runners = []
    for num_workers in range (min_workers, max_workers, step):
        if num_workers==0:
            continue
        #tasks = [task_class(*args, **kwargs) for x in range(num_workers)]
        kwargs["number_of_workers"]=num_workers
        tasks = [task_class(*args, **kwargs)] *num_workers
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


