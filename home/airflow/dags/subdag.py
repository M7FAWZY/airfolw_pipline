from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.subdag_operator import SubDagOperator
from helpers import SqlQueries

#subDAG for load_dimension_table
def load_dimension__table(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        insert_sql_stmt,
        s3_bucket,
        s3_key,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    

    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=conn_id,
        sql=create_sql_stmt
    )


    load_dimension_table_task = LoadDimensionOperator(
        task_id=f"load_{table}_dimension_table",
        dag=dag,
        target_table=target_table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key
    )

    insert_task = PostgresOperator(
        task_id=f"insert_{table}_table",
        dag=dag,
        postgres_conn_id=conn_id,
        sql=insert_sql_stmt
    )    

    #subDAg dependencies
    create_task >> load_dimension_table_task >> insert_task
    return dag