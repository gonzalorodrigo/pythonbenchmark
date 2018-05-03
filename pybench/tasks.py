from pybench.runner import TaskObject
from pybench.perf import TM
from math import sqrt
import psycopg2



class CPUTask(TaskObject):

    def __init__(self, reps=10000):
        self._reps=reps

    def do_op(self, worker_id, debug=False):
        for i in range(self._reps):
            self._n=sqrt(2**2*2**2+2**2*2**2+2)


class PSQLTask(TaskObject):

    def __init__(self, rows=10000, reps=10, table="table_name",
                db_host="hostname", db_name="db_name", user="user", 
                password=""):
        self._rows = rows
        self._reps = reps
        self._table = table
        self._db_host = db_host
        self._db_name = db_name
        self._user = user
        self._password = password


    def do_op(self, worker_id, debug=False):
        conn = psycopg2.connect(dbname=self._db_name,
                                host=self._db_host,
                                user=self._user,
                                password=self._password)
        cur = conn.cursor()
        for i in range(self._reps):
            base_start = i*self._rows
            cur.execute("SELECT * from {} LIMIT {} OFFSET {}".format(
                self._table, self._rows, base_start))

        cur.close()
        conn.close()



    






