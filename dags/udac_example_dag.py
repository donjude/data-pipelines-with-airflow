from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
<<<<<<< HEAD
=======
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    'email_on_retry': False,
    'catchup': False    
>>>>>>> origin/main
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
<<<<<<< HEAD
=======
          max_active_runs=1
>>>>>>> origin/main
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
>>>>>>> origin/main
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    extra_params="JSON 'auto' COMPUPDATE OFF"
>>>>>>> origin/main
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
>>>>>>> origin/main
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate_table=True
>>>>>>> origin/main
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate_table=True
>>>>>>> origin/main
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    table=SqlQueries.artist_table_insert,
    truncate_table=True
>>>>>>> origin/main
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
<<<<<<< HEAD
    dag=dag
=======
    dag=dag,
    redshift_conn_id="redshift",
    table="time,
    sql=SqlQueries.time_table_insert,
    truncate_table=True
>>>>>>> origin/main
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
<<<<<<< HEAD
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
=======
    dag=dag,
    redshift_conn_id="redshift",
    sql_null_checks=SqlQueries.null_checks,
    expected_results=SqlQueries.exp_results
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

>>>>>>> origin/main
