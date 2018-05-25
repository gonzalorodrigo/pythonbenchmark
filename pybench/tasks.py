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
                password="", id_field="id"):
        self._rows = rows
        self._reps = reps
        self._table = table
        self._db_host = db_host
        self._db_name = db_name
        self._user = user
        self._password = password
        self._id_field = id_field


    def do_op(self, worker_id, debug=False):
        conn = psycopg2.connect(dbname=self._db_name,
                                host=self._db_host,
                                user=self._user,
                                password=self._password)
        cur = conn.cursor()
        for i in range(self._reps):
            base_start = i*self._rows
            cur.execute("SELECT * from {} ORDER BY {} LIMIT {} OFFSET {}"
                "".format(
                self._table, self._id_field, self._rows, base_start))
            cur.fetchall()

        cur.close()
        conn.close()


class PSQLTaskAllTable(TaskObject):

    def __init__(self, reps=1, number_of_workers=1,
                table="table_name",
                db_host="hostname", db_name="db_name", user="user", 
                password="", id_field="id",
                inverse=False):
        self._reps = reps
        self._table = table
        self._db_host = db_host
        self._db_name = db_name
        self._user = user
        self._password = password
        self._id_field = id_field
        self._number_of_workers=number_of_workers
        self._inverse = inverse
        conn = psycopg2.connect(dbname=self._db_name,
                                host=self._db_host,
                                user=self._user,
                                password=self._password)
        cur = conn.cursor()
        cur.execute("SELECT count(*) from {}"
                "".format(
                self._table))
        result = cur.fetchone()
        self._number_rows_in_table=result[0]
        cur.close()
        conn.close()

        self._rows = int(self._number_rows_in_table/self._number_of_workers)
        print("All Table retrieve exp init: Table {}, rows {},"
            " rows per worker {}".format(self._table,
                self._number_rows_in_table,
                self._rows))


    def do_op(self, worker_id, debug=False):
        conn = psycopg2.connect(dbname=self._db_name,
                                host=self._db_host,
                                user=self._user,
                                password=self._password)
        cur = conn.cursor()
        for i in range(self._reps):
            if self._inverse:
                base_start = (self._number_of_workers-1-worker_id)*self._rows
            else:
                base_start = worker_id*self._rows
            if debug:
                print ("Worker {}, retrieve rows {}-{}".format(worker_id,
                    base_start, base_start+self._rows))
            cur.execute("SELECT * from {} ORDER BY {} LIMIT {} OFFSET {}"
                "".format(
                self._table, self._id_field, self._rows, base_start))
            cur.fetchall()

        cur.close()
        conn.close()


    






