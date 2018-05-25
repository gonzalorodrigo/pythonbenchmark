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
       
        self._number_rows_in_table = self._get_num_rows()
        self._rows = int(self._number_rows_in_table/self._number_of_workers)
        print("All Table retrieve exp init: Table {}, rows {},"
            " rows per worker {}".format(self._table,
                self._number_rows_in_table,
                self._rows))

    def _get_num_rows_query(self):
        return ("SELECT count(*) from {}"
                "".format(
                self._table))
    def _get_num_rows(self):
        conn = psycopg2.connect(dbname=self._db_name,
                                host=self._db_host,
                                user=self._user,
                                password=self._password)
        cur = conn.cursor()
        cur.execute(self._get_num_rows_query())
        result = cur.fetchone()
        num_rows=result[0]
        cur.close()
        conn.close()
        return num_rows


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


class PSQLTaskAllTableID(PSQLTaskAllTable):

    def _get_num_rows_query(self):
        return ("SELECT max({}) from {}"
                "".format(
                self._id_field,
                self._table))
    
    def _get_op_query(self, base_start, base_end):
        return ("SELECT * from {} "
                        " WHERE {} >= {}  and {} < {}"
                "".format(
                self._table,
                self._id_field, base_start,
                self._id_field, base_end))

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
            base_end = base_start+self._rows
            if debug:
                print ("Worker {}, retrieve rows {} >= {}"
                    " and {} < {}".format(worker_id,
                        self._id_field, base_start,
                        self._id_field, base_end))

            op_query = self._get_op_query(base_start, base_end)
            if debug:
                print("Worker {} Query: {}".format(worker_id, op_query))
            cur.execute(op_query)
            result=cur.fetchall()
            if debug:
                print("Worker {}: {} rows retrieved".format(worker_id,
                    len(result)))

        cur.close()
        conn.close()

    






