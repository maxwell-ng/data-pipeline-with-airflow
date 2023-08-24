from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 iam_role="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 sql_copy="",
                 sql_create_table="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_copy = sql_copy
        self.sql_create_table = sql_create_table
        self.iam_role = iam_role
        self.json = json

    def execute(self, context):
        self.log.info('Getting AWS credentials')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Dropping table ({self.table_name}) in Redshift")
        redshift.run(f"DROP TABLE IF EXISTS {self.table_name};")
        
        self.log.info(f"Creating table ({self.table_name}) in Redshift")
        redshift.run(self.sql_create_table)
        
        self.log.info("Copying data from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        formatted_sql = self.sql_copy.format(
            self.table_name,
            s3_path,
            self.iam_role,
            self.json
        )
        redshift.run(formatted_sql)
