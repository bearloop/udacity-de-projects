from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # template_fields = ('s3_key',)
    
    copy_sql = """
                   COPY {}
                   FROM '{}'
                   ACCESS_KEY_ID '{}'
                   SECRET_ACCESS_KEY '{}'
                   JSON '{}'
               """
    
    @apply_defaults
    def __init__(self,

                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 *args,
                 **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        
        
#'s3://udacity-dend/log_json_path.json'
    def execute(self, context):
        
        
        # Get AWS credentials and make connection to Redshift
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Drop from Redshift existing tables of the same name as passed table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Render S3 key
        rendered_key = self.s3_key.format(**context)
        
        # Determine S3 path
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        #self.log.info(s3_path)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
        
        self.log.info("Trying to copy data from S3 to Redshift")
        redshift.run(formatted_sql)
            