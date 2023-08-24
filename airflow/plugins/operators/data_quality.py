from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_queries=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries = sql_queries

    def execute(self, context):
        self.log.info('Establishing connection with Redshift cluster.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting data quality checks.')
        for i in self.sql_queries:
            if redshift.get_records(i):
                raise ValueError(f"The following query: ({i}) returned unexpected results")
            
