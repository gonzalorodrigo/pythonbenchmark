from pybench.runner import run_experiment
from pybench.tasks import CPUTask

reps = 10000000

run_experiment("CPU BOUND", CPUTask, reps=10000000)


