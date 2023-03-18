root@a2e5e2482542:/home/workspace# /opt/airflow/start.sh
[2023-03-18 13:04:35,532] {settings.py:174} INFO - settings.configure_orm(): Using pool settings. pool_size=5, pool_recycle=1800, pid=328
[2023-03-18 13:04:37,206] {__init__.py:51} INFO - Using executor LocalExecutor
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 
[2023-03-18 13:04:38,928] {settings.py:174} INFO - settings.configure_orm(): Using pool settings. pool_size=5, pool_recycle=1800, pid=334
[2023-03-18 13:04:40,509] {__init__.py:51} INFO - Using executor LocalExecutor
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 
[2023-03-18 13:04:41,455] {models.py:273} INFO - Filling up the DagBag from /home/workspace/airflow/dags
[2023-03-18 13:04:42,994] {models.py:377} ERROR - Failed to import: /home/workspace/airflow/dags/udac_example_dag.py
Traceback (most recent call last):
  File "/opt/conda/lib/python3.6/site-packages/airflow/models.py", line 374, in process_file
    m = imp.load_source(mod_name, filepath)
  File "/opt/conda/lib/python3.6/imp.py", line 172, in load_source
    module = _load(spec)
  File "<frozen importlib._bootstrap>", line 684, in _load
  File "<frozen importlib._bootstrap>", line 665, in _load_unlocked
  File "<frozen importlib._bootstrap_external>", line 678, in exec_module
  File "<frozen importlib._bootstrap>", line 219, in _call_with_frames_removed
  File "/home/workspace/airflow/dags/udac_example_dag.py", line 114, in <module>
    start_date=default_args['start_date'],
  File "/home/workspace/airflow/dags/subdag.py", line 47, in load_dimension_subdag
    s3_key=s3_key
  File "/opt/conda/lib/python3.6/site-packages/airflow/utils/decorators.py", line 94, in wrapper
    raise AirflowException(msg)
airflow.exceptions.AirflowException: Argument ['drop_table', 'create_query', 'insert_query', 'append'] is required
Running the Gunicorn Server with:
Workers: 4 sync
Host: 0.0.0.0:3000
Timeout: 120
Logfiles: - -
=================================================================            
Waiting for Airflow web server...
Airflow web server is ready
