from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_insert="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert = sql_insert
        self.append = append

    def execute(self, context):
        self.log.info('Establishing connection with Redshift cluster.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append:
            try:
                self.log.info(f"Clearing data from destination Redshift table: {self.table_name}")
                redshift.run(f"DELETE FROM {self.table_name}")
            except Exception:
                self.log.info(f"The table {self.table_name} does not exist.")
        
        try:
            self.log.info(f"Inserting data in dimension table ({self.table_name}) in Redshift.")
            insert_stm = f"INSERT INTO {self.table_name} ({self.sql_insert});"
            redshift.run(insert_stm)
        except Exception as err:
            self.log.info(f"Task failed because of: {str(err)}")
            