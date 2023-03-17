from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import pytest
from airflow.models import DagBag

#Operator that runs data checks against the inserted data
class DataQualityOperator(BaseOperator):
#Operator that runs data checks against the inserted data

    ui_color = '#89DA59'

    @apply_defaults
                 # Define your operators params (with defaults) below
                 # Example:
                 # conn_id = your-connection-name
    def __init__(self,                 
                 redshift_conn_id='redshift',
                 #target_tables=[],
                 check_stmts=[],
                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        #self.target_tables = target_tables
        self.check_stmts = check_stmts
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
    
    #Used control in data quality: Static control
    #<https://docs.astronomer.io/learn/data-quality#types-of-data-quality-checks>
    
           
    def execute(self, context):
        
        # From reviewer and  <https://docs.astronomer.io/learn/airflow-sql-data-quality>
        # also <https://knowledge.udacity.com/questions/552650>
        for stmt in self.check_stmts:
            result = int(redshift_hook.get_first(sql=stmt['sql'])[0])
            # check if equal
            if stmt['op'] == 'eq':
                if result != stmt['val']:
                    raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
            # check if not equal
            elif stmt['op'] == 'ne':
                if result == stmt['val']:
                    raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
            # check if greater than
            elif stmt['op'] == 'gt':
                if result <= stmt['val']:
                    raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
            self.log.info(f"Passed check: {result} {stmt['op']} {stmt['val']}")
                
        
        