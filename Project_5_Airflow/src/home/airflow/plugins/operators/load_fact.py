from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 query = "",
                 table="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.query = query
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        
        self.log.info('LoadFactOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
          
        sql_query = 'INSERT INTO {} {}'.format(self.table, self.query)

        self.log.info('Fact table load is starting')
        
        redshift.run(sql_query)

        self.log.info('Fact table load is completed')