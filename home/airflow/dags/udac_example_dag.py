#Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG.

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.subdag_operator import SubDagOperator
from subdag import load_dimension_subdag
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
start_date = datetime.utcnow()
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=2),
    'catchup_by_default': False,
    'email_on_retry': False    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #https://crontab.guru/
          #http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html
          schedule_interval='0 * * * *',#@hourly or '0 0-23 * * *'
          
          catchup = False,
          max_active_runs=1
          #delay_on_limit_secs=2          
          
        )

#Dummy opertator only represent start of DAG tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    drop_table=True,
    
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    
    create_query=SqlQueries.create_staging_events_table,
    s3_bucket="udacity-dend",
    s3_key="log_data",
    # [udacity knowldege] (https://knowledge.udacity.com/questions/556140)
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    execution_date=start_date
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table='staging_songs',
    drop_table=True,
    
    aws_connection_id='aws_credentials',
    redshift_conn_id='redshift',
    
    create_query=SqlQueries.create_staging_songs_table,
    s3_bucket='udacity-dend',
    s3_key='song_data',
    extra_params="FORMAT AS json 'auto'",
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_connection_id='redshift',
    target_table='songplays',
    drop_table=True,
    create_query=SqlQueries.create_songplays_table,
    insert_query=SqlQueries.songplay_table_insert,
    conn_id='redshift',
    append=True#Do not use append=False ; Need append in fact table
    #start_date=start_date
)

#use SubDAG to load_dimension_table <https://knowledge.udacity.com/questions/302530>
load_user_dimension_task="Load_user_dimension_table"
load_user_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='udac_example_dag',
        task_id=load_user_dimension_task,
        redshift_conn_id="redshift",
        aws_credentials_id='aws_credentials',
        
        table = 'users',
        
        create_query=SqlQueries.create_users_table,       
        insert_query=SqlQueries.user_table_insert,
        
        s3_bucket="udacity-dend",
        s3_key="song_data",
        delete_load = True,       
        start_date=default_args['start_date'],
        #start_date=start_date,
    ),
    task_id=load_user_dimension_task,
    dag=dag,
)

load_song_dimension_task ="Load_song_dimension_table"
load_song_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='udac_example_dag',
        task_id=load_user_dimension_task,
        redshift_conn_id="redshift",
        aws_credentials_id='aws_credentials',
        
        table = 'songs',
        
        create_query=SqlQueries.create_users_table,       
        insert_query=SqlQueries.user_table_insert,
        
        s3_bucket="udacity-dend",
        s3_key="song_data",
        delete_load = True,       
        start_date=default_args['start_date'],
                
        #start_date=start_date,
    ),
    task_id=load_song_dimension_task,
    dag=dag,
)
load_artist_dimension_task = "Load_artist_dimension_table"
load_artist_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='udac_example_dag',
        task_id=load_user_dimension_task,
        redshift_conn_id="redshift",
        aws_credentials_id='aws_credentials',
        
        table = 'artists',
        
        create_query=SqlQueries.create_users_table,       
        insert_query=SqlQueries.user_table_insert,
        
        s3_bucket="udacity-dend",
        s3_key="song_data",
        delete_load = True,       
        start_date=default_args['start_date'],
         
        #start_date=start_date,
    ),
    task_id=load_artist_dimension_task,
    dag=dag,
)

load_time_dimension_task="Load_time_dimension_table"
load_time_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='udac_example_dag',
        task_id=load_user_dimension_task,
        redshift_conn_id="redshift",
        aws_credentials_id='aws_credentials',
        
        table = 'time',
        
        create_query=SqlQueries.create_users_table,       
        insert_query=SqlQueries.user_table_insert,
        
        s3_bucket="udacity-dend",
        s3_key="song_data",
        delete_load = True,       
        start_date=default_args['start_date'],
         
        #start_date=start_date,
    ),
    task_id=load_time_dimension_task,
    dag=dag,
)

"""
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
    conn_id='redshift',
    target_table='users',
    drop_table=True,
    create_query=SqlQueries.create_users_table,
    insert_query=SqlQueries.user_table_insert,
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
    conn_id='redshift',
    target_table='songs',
    drop_table=True,
    create_query=SqlQueries.create_songs_table,
    insert_query=SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
    conn_id='redshift',
    target_table='artists',
    drop_table=True,
    create_query=SqlQueries.create_artist_table,
    insert_query=SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
    conn_id='redshift',
    target_table='time',
    drop_table=True,
    create_query=SqlQueries.create_time_table,
    insert_query=SqlQueries.time_table_insert,
    append=False
)
"""
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    #<https://knowledge.udacity.com/questions/556678>    
    #check_count_sql=default_args['check_count_sql'],  
    #check_null_sql=default_args['check_null_sql'],
    check_stmts=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL;',
            'op': 'eq',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM song ;',
            'op': 'eq',
            'val': 0
        },        
        {
            'sql': 'SELECT COUNT(*) FROM songid WHERE id IS NULL;',
            'op': 'eq',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM users;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL;',
            'op': 'eq',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM artists;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL;',
            'op': 'eq',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM time;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL;',
            'op': 'eq',
            'val': 0
        },
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Task dependencies
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
[stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks 


run_quality_checks >> end_operator

