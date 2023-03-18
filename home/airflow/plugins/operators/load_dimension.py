from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Operator that loads data from staging table to the dimension table
class LoadDimensionOperator(BaseOperator):
#Operator that loads data from staging table to the dimension table

    ui_color = '#80BD9E'

    @apply_defaults
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
    def __init__(self,                 
                 redshift_conn_id,
                 aws_credentials_id,                 
                 
                 create_query,
                 insert_query,
                 
                 table,
                 
                 drop_table,               
                 append,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.target_table = table
        self.drop_table = drop_table
        self.create_query = create_query
        self.insert_query = insert_query
        self.append = append

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        if self.drop_table:
            self.log.info('Dropping {} table if it exists...'.format(
                self.target_table))
            self.hook.run("DROP TABLE IF EXISTS {}".format(self.target_table))
            self.log.info(
                "Table {} has been successfully dropped".format(
                    self.target_table))

        self.log.info(
            'Creating {} table if it does not exist...'.format(
                self.target_table))
        self.hook.run(self.create_query)
        if not self.append:
            self.log.info("Removing data from {}".format(self.target_table))
            self.hook.run("DELETE FROM {}".format(self.target_table))

        self.log.info('Inserting data from staging table...')
        self.hook.run(self.insert_query)
        self.log.info("Insert execution complete...")
