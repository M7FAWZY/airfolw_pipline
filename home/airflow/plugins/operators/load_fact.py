 from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Operator that loads data from staging table to the fact table
class LoadFactOperator(BaseOperator):
#Operator that loads data from staging table to the fact table

    ui_color = '#F98866'

    @apply_defaults
                 # Define your operators params (with defaults) below
                 # Example:
                 # conn_id = your-connection-name
    def __init__(self,                
                 redshift_conn_id='',
                 aws_credentials_id='',
                 
                 target_table='',
                 create_query='',
                 insert_query='',
                 drop_table=True,
                 append=True,
                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.drop_table = drop_table
        self.target_table = target_table
        self.create_query = create_query
        self.insert_query = insert_query
        self.append = append
        
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        if self.drop_table:
            self.log.info('Dropping {} table if it Exists...'.format(
                self.target_table))
            self.hook.run("DROP TABLE IF EXISTS {}".format(self.target_table))
            self.log.info(
                "Table {} has been successfully dropped".format(
                    self.target_table))

        self.log.info(
            'Creating {} table if it does not Exist...'.format(
                self.target_table))
        self.hook.run(self.create_query)

        if not self.append:
            self.log.info("Removing data from {}".format(self.target_table))
            self.hook.run("DELETE FROM {}".format(self.target_table))

        self.log.info('Inserting data from staging table...')
        self.hook.run(self.insert_query)
        self.log.info("Insert execution complete...")
         
