import pendulum
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.redshift_custom_operator import PostgreSQLOperator

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'Nicole C.',  # Owner of the DAG
    'start_date': pendulum.now(),  # Start date for the DAG execution
    'depends_on_past': False,  # Tasks do not wait for previous DAG runs to succeed
    'retries': 3,  # Number of retries on failure
    'retry_delay': timedelta(minutes=5),  # Time interval between retries
    'catchup': False,  # Prevent backfilling for missed runs
    'email_on_retry': False  # Disable email notifications on retries
}

# Define the DAG using the Airflow @dag decorator
@dag(
    default_args=default_args,  # Apply default arguments
    description='Create tables in Redshift with Airflow',  # Description of the DAG
    schedule_interval='0 * * * *'  # Schedule to run the DAG every hour
)
def create_tables():
    """
    DAG to create tables in Redshift using Airflow.
    """

    # DummyOperator to mark the beginning of the DAG
    start_operator = DummyOperator(task_id='Begin_execution')

    # Task to execute SQL script for creating tables in Redshift
    create_redshift_tables = PostgreSQLOperator(
        task_id='Create_tables',  # Task ID for the operator
        postgres_conn_id='redshift',  # Connection ID for Redshift
        sql='create_tables.sql'  # Path to the SQL file containing the table creation statements
    )

    # DummyOperator to mark the end of the DAG
    end_operator = DummyOperator(task_id='Stop_execution')

    # Define the task dependencies
    start_operator >> create_redshift_tables >> end_operator

# Instantiate the DAG
create_tables_dag = create_tables()
