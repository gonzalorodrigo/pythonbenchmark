from pybench.runner import run_experiment
from pybench.tasks import (CPUTask, PSQLTask,PSQLTaskAllTable,
    PSQLTaskAllTableID)
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
parser.add_argument("-d", "--idpsql", action="store_true",
                    help="PSQL test retrieving all rows using where ID")

parser.add_argument("-i", "--inverse", action="store_true",
                    help="if set, AllPSQL test will asign rows in inverse"
                    " order to woker_id")



args = parser.parse_args()
max_workers =  int(args.num_workers)
worker_step=int(args.step)
if args.cpu:
    run_experiment("CPU BOUND", CPUTask, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=10000000)
if args.psql or args.allpsql or args.idpsql:
    username, password = get_pg_data(username="postgres")
if args.psql:
    run_experiment("PSQL", PSQLTask, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=10,
        rows=100000,
        table="searchengine_scopeimagemetadata",
        db_host="db", db_name="metadataserver", 
        user=username, password=password)
if args.allpsql:
    run_experiment("AllPSQL", PSQLTaskAllTable, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=1,
        table="searchengine_scopeimagemetadata",
        db_host="db", db_name="metadataserver", 
        user=username, password=password,
        inverse=args.inverse)
if args.idpsql:
    run_experiment("IdPSQL", PSQLTaskAllTableID, max_workers=max_workers, 
        step=worker_step, debug=args.verbose, reps=1,
        table="searchengine_scopeimagemetadata",
        db_host="db", db_name="metadataserver", 
        user=username, password=password,
        inverse=args.inverse)


