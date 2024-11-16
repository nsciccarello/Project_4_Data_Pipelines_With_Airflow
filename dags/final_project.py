from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

# Custom operators for staging, loading, and data quality
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

# SQL queries for ETL processes
from udacity.common.final_project_sql_statements import SqlQueries

# Default arguments for the DAG
default_args = {
    'owner': 'Nicole C.',                      # Owner of the DAG
    'depends_on_past': False,               # Tasks do not depend on previous runs
    'start_date': datetime(2018, 11, 1),    # Start date for DAG execution
    'retries': 3,                           # Number of retries on failure
    'retry_delay': timedelta(minutes=5),    # Delay between retries
    'catchup': False,                       # Prevent backfilling for missed runs
    'email_on_retry': False                 # Disable email notifications on retries
}

# Define the DAG using the Airflow @dag decorator
@dag(
    default_args=default_args,                 # Apply default arguments
    description='Load and transform data in Redshift with Airflow',  # DAG description
    end_date=datetime(2018, 11, 2),            # End date for the DAG
    schedule_interval='0 * * * *'             # Schedule interval (every hour)
)
def final_project():
    """
    DAG to load and transform data in Redshift using Airflow.
    """

    # Dummy task to mark the start of the DAG
    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage events data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='data-pipelines-project',
        s3_key='log-data',
        log_json_file='log_json_path.json'  # JSON path file for log data
    )

    # Stage songs data from S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='data-pipelines-project',
        s3_key='song-data/A/A/'  # Key for the song data files
    )

    # Load fact table (songplays) with transformed data
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert  # SQL query for inserting data
    )

    # Load user dimension table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,    # SQL query for inserting data
        mode='truncate-insert'                    # Truncate table before insert
    )

    # Load song dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        mode='truncate-insert'
    )

    # Load artist dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        mode='truncate-insert'
    )

    # Load time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        mode='truncate-insert'
    )

    # Run data quality checks on all tables
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']  # List of tables to check
    )

    # Dummy task to mark the end of the DAG
    end_operator = DummyOperator(task_id='Stop_execution')

    # Define task dependencies to establish the data pipeline flow
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
        load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
        run_quality_checks >> end_operator


# Instantiate the DAG
final_project_dag = final_project()
