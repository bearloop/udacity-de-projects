from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 query = "",
                 table="",
                 redshift_conn_id="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.query = query
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
          
        if self.truncate:
            
            self.log.info('Trucating table')
                
            redshift.run("TRUNCATE {}".format(self.table))
            
            self.log.info('Trucating table completed')
            
        sql_query = 'INSERT INTO {} {}'.format(self.table, self.query)

        self.log.info('{} table load is starting'.format(self.table))
        
        redshift.run(sql_query)

        self.log.info('{} table load is completed'.format(self.table))
