from pybench.runner import run_experiment
from pybench.tasks import CPUTask, PSQLTask
from pybench.support import get_pg_data
import argparse

reps = 10000000

parser = argparse.ArgumentParser()
parser.add_argument("num_workers", help="max number of workers")
parser.add_argument("-s", "--step", default="1",
                    help="workers increase step")

parser.add_argument("-v", "--verbose", action="store_true",
                    help="increase output verbosity")

parser.add_argument("-c", "--cpu", action="store_true",
                    help="CPU bound test")

parser.add_argument("-p", "--psql", action="store_true",
                    help="PSQL test")
parser.add_argument("-a", "--allpsql", action="store_true",
                    help="PSQL test retrieving all rows")


args = parser.parse_args()
max_workers =  int(args.num_workers)
worker_step=int(args.step)
if args.cpu:
    run_experiment("CPU BOUND", CPUTask, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=10000000)
if args.psql or args.allpsql:
    username, password = get_pg_data(username="postgres")
if args.psql:
    run_experiment("PSQL", PSQLTask, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=10,
        rows=100000,
        table="searchengine_scopeimagemetadata",
        db_host="db", db_name="metadataserver", 
        user=username, password=password)
if args.allpsql:
    username, password = get_pg_data(username="postgres"):
    run_experiment("PSQL", PSQLTask, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=1,
        number_of_workers=max_workers,
        table="searchengine_scopeimagemetadata",
        db_host="db", db_name="metadataserver", 
        user=username, password=password)



