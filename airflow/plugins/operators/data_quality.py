from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_sql_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_sql_checks = dq_sql_checks

    def execute(self, context):
        self.log.info('Establishing connection with Redshift cluster.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting data quality checks.')
        for check in self.dq_sql_checks:
            result = redshift.get_records(check['check_sql'])
            if result != check['expected_result']:
                raise ValueError(f"The following query: ({check['check_sql']}) returned unexpected results")
            self.log.info(f"Data quality check for the for following query: {check['check_sql']}, returned expected result: {check['expected_result']}")
