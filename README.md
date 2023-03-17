# airfolw_pipline
problem of run <code>
root@e07e5fd54b03:/home/workspace# /opt/airflow/start.sh
[2023-03-17 16:35:57,156] {settings.py:174} INFO - settings.configure_orm(): Using pool settings. pool_size=5, pool_recycle=1800, pid=1529
[2023-03-17 16:35:57,867] {__init__.py:51} INFO - Using executor LocalExecutor
 </code> <code> ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 
[2023-03-17 16:35:59,352] {settings.py:174} INFO - settings.configure_orm(): Using pool settings. pool_size=5, pool_recycle=1800, pid=1547
[2023-03-17 16:36:00,915] {__init__.py:51} INFO - Using executor LocalExecutor`
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
</code> <code>
[2023-03-17 16:36:01,856] {models.py:273} INFO - Filling up the DagBag from /home/workspace/airflow/dags
[2023-03-17 16:36:01,868] {models.py:377} ERROR - Failed to import: /home/workspace/airflow/dags/udac_example_dag.py
Traceback (most recent call last):
  File "/opt/conda/lib/python3.6/site-packages/airflow/models.py", line 374, in process_file
    m = imp.load_source(mod_name, filepath)
  File "/opt/conda/lib/python3.6/imp.py", line 172, in load_source
    module = _load(spec)
  File "<frozen importlib._bootstrap>", line 684, in _load
  File "<frozen importlib._bootstrap>", line 665, in _load_unlocked
  File "<frozen importlib._bootstrap_external>", line 678, in exec_module
  File "<frozen importlib._bootstrap>", line 219, in _call_with_frames_removed
  File "/home/workspace/airflow/dags/udac_example_dag.py", line 86, in <module>
    insert_query=SqlQueries.songplay_table_insert
  File "/opt/conda/lib/python3.6/site-packages/airflow/utils/decorators.py", line 94, in wrapper
    raise AirflowException(msg)
airflow.exceptions.AirflowException: Argument ['append', 'conn_id', 'drop_table'] is required
Running the Gunicorn Server with:
Workers: 4 sync
Host: 0.0.0.0:3000
Timeout: 120
Logfiles: - -
=================================================================            
Waiting for Airflow web server...
Airflow web server is ready
</code>
