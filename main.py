from pybench.runner import TaskRunner
from pybench.tasks import CPUTask

reps = 10000000
task_runners = []
for num_workers in range (1, 16):
    tasks = [CPUTask(reps=reps) for x in range(num_workers)]
    tr = TaskRunner(num_workers, tasks)
    task_runners.append(tr)
    print("Running exp({})".format(num_workers))
    tr.run_tasks()
    print("Exp({}) run: {}".format(num_workers, tr.get_result_summary()))

print("Task runtime per worker")
first=True
for tr in task_runners:
    if first:
        print(tr.get_header())
        first=False
    print(tr.get_result_summary())


