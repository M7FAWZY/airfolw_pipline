from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries
import sql
#subDAG for load_dimension_table
def load_dimension_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_query,
        insert_query,
        s3_bucket,
        s3_key,
        delete_load,        
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    

    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_query
    )


    load_dimension_table_task = LoadDimensionOperator(
        task_id=f"load_{table}_dimension_table",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key
    )

    insert_task = PostgresOperator(
        task_id=f"insert_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=insert_query
    )    

#subDAg dependencies
    create_task >> load_dimension_table_task >> insert_task
    return dag
