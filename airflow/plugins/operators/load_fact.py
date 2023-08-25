from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_insert="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert = sql_insert

    def execute(self, context):
        self.log.info('Establishing connection with Redshift cluster.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        try:
            self.log.info(f"Inserting data in dimension table ({self.table_name}) in Redshift.")
            insert_stm = f"INSERT INTO {self.table_name} ({self.sql_insert});"
            redshift.run(insert_stm)
        except Exception as err:
            self.log.info(f"Task failed because of: {str(err)}")
