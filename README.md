pybench is a benchmarking tool to evaluate process parallelism performance in
Python. It allows to run N tasks in parallel and measure their runtime. In
includes base classes to define new Task types and extend its functionalities.

**Benchmarks**
- CPU benchmark: N parallel tasks running CPU intensive code.
- PSQL benchmark: N parallel PostgreSQL client processes running each one a
query agains a database.

PSQL env vars:
~~~
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_PASSWORD_FILE
~~~

**Examples**

~~~
python main.py --cpu 8
~~~
It will run 8 CPU intensive benchmarks with 1,2,3,4,5,6,7,8 parallel processes.

~~~
python main.py --pqsl 8
~~~
It will run 8 benchmarks with 1,2,3,4,5,6,7,8 parallel PSQL queries.

~~~
python main.py --pqsl --step 2 8
~~~
It will run 4 benchmarks with 2,4,6,8 parallel PSQL queries.
