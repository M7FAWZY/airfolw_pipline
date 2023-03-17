from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Operator that loads any JSON formatted files from S3 to Amazon Redshift
class StageToRedshiftOperator(BaseOperator):
#Operator that loads any JSON formatted files from S3 to Amazon Redshift
    
    ui_color = '#358140'
    
    template_fields = ("s3_key",)    
    copy_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL 
        {} 'auto'
        {}
    """
    #In case of csv file
    
    @apply_defaults
    
                 # Define your operators params (with defaults) below
                 # Example:
                 # redshift_conn_id=your-connection-name
    #to resolve SyntaxError: non-default argument follows default argument
    #<https://knowledge.udacity.com/questions/967994>
    #<https://www.pythonclear.com/errors/non-default-argument/>
    #<https://sebhastian.com/syntaxerror-non-default-argument-follows-default-argument-python/#:~:text=A%20SyntaxError%3A%20non%2Ddefault%20argument,defined%20before%20arguments%20with%20defaults.>
    def __init__(self,        
                 table="",
                 drop_table =False,
                 aws_connection_id="",
                 redshift_connection_id="",
                 region= "",
                 
                 #create_query,
                 s3_bucket="",
                 s3_key="",
                 extra_params="JSON",
        
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        self.table = table
        self.drop_table = drop_table
        self.aws_connection_id = aws_connection_id
        self.redshift_connection_id = redshift_connection_id
        self.region = region
        
        #self.create_query = create_query        
        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_params = extra_params
        #self.data_format = data_format
        self.execution_date = kwargs.get('execution_date')
        
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        self.aws_instance = AwsHook(aws_conn_id=self.aws_connection_id)
        credentials = self.aws_instance.get_credentials()
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_query = StageToRedshiftOperator.copy_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_params,
            self.execution_date.strftime("%Y"),
            self.execution_date.strftime("%d"),
            self.data_format,
            self.execution_date
        )
        
        #In case of csv file
        if self.extra_params == 'CSV':
            self.log.info('self.extra_params is CSV')
            raise alert ("self.extra_params is 'CSV'")
            copy_query_csv="""
                   COPY {}
                   FROM '{}'
                   ACCESS_KEY_ID '{}'
                   SECRET_ACCESS_KEY '{}'
                   REGION '{}'
                   TIMEFORMAT as 'epochmillisecs'
                   TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL 
                   {} 'auto'
                   {}
                   delimiter ','
                   ignoreheader 1
                   CSV quote as '"';
              """%self.table_name,s3_path,self.aws_credentials.get('key'),self.aws_credentials.get('secret')
            copy_query=copy_query_csv
            self.log.info('copy query changed from CSV to JSON.')
            
        if self.drop_table:
            self.log.info('Dropping {} table if it exists...'.format(
                self.table))
            self.hook.run("DROP TABLE IF EXISTS {}".format(self.table))
            self.log.info(
                "Table {} has been successfully dropped".format(
                    self.table))
            
        self.log.info(
            'Creating {} table if it does not exist...'.format(self.table))
        self.hook.run(self.create_query)
        self.log.info("Removing data from {}".format(self.table))
        self.hook.run("DELETE FROM {}".format(self.table))
        self.log.info('Executing copy query...')
        self.hook.run(formatted_query)
        self.log.info("copy query execution complete...")
        self.execution_date.strftime("%Y"),
        self.execution_date.strftime("%d"),
        self.data_format,
        self.execution_date