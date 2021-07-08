from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 dq_checks={},
                 tables='',
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.dq_checks=dq_checks
        self.tables=tables
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        i = 0 
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            table = self.tables[i]
            
            records = redshift.get_records(sql)[0]
            # compare with the expected results
            if len(records) != exp_result:
                raise ValueError(f"Data quality check failed. {table} returned {len(records)} rows instead of {exp_result}.")
              
            else:
                logging.info(f"Data quality on table {table} check passed with {exp_result} rows")
            
            i+=1
            
#         for table in self.tables:
            
#             sql_query = "SELECT COUNT(*) FROM {} WHERE {} is null".format(table, col[table])            
#             records = redshift_hook.get_records(sql_query)
            
#             if len(records) < 1 or len(records[0]) < 1:
#                 raise ValueError(f"Data quality check failed. {table} returned no results")
                
#             num_records = records[0][0]
#             if num_records < 1:
#                 raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                
#             logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")