from datetime import datetime, timedelta
import pendulum
import os
import sys
sys.path.append("/opt/airflow/")
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 8, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None
)
def data_pipeline():
    sql_queries = SqlQueries()
    
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        iam_role=Variable.get("iam_role"),
        table_name="staging_events",
        s3_bucket="sparkify-streaming-song-datalake",
        s3_key="log-data",
        sql_copy=sql_queries.sql_copy,
        sql_create_table=sql_queries.staging_events_create,
        json='auto',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        iam_role=Variable.get("iam_role"),
        table_name="staging_songs",
        s3_bucket="sparkify-streaming-song-datalake",
        s3_key="song-data/A/A/A",
        sql_copy=sql_queries.sql_copy,
        sql_create_table=sql_queries.staging_songs_create,
        json="auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table_name="songplays",
        sql_create=sql_queries.songplay_table_create,
        sql_insert=sql_queries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table_name="users",
        sql_create=sql_queries.user_table_create,
        sql_insert=sql_queries.user_table_insert,
        append=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table_name="songs",
        sql_create=sql_queries.song_table_create,
        sql_insert=sql_queries.song_table_insert,
        append=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table_name="artists",
        sql_create=sql_queries.artist_table_create,
        sql_insert=sql_queries.artist_table_insert,
        append=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table_name="time",
        sql_create=sql_queries.time_table_create,
        sql_insert=sql_queries.time_table_insert,
        append=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        sql_queries=sql_queries.dq_query_list,
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

data_pipeline_dag = data_pipeline()