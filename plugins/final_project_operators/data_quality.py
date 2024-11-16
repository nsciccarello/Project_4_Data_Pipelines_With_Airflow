from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Custom Airflow operator to perform data quality checks on Redshift tables.
    """
    ui_color = '#89DA59'  # Sets the UI color for the operator in Airflow's DAG view.

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=None,
                 *args, **kwargs):
        """
        Initializes the operator with required parameters.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param tables: List of tables to perform data quality checks on
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Executes data quality checks on each specified table.

        :param context: Airflow execution context (not used in this implementation)
        :raises ValueError: If any table fails the data quality check
        """
        # Establish connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Iterate over each table and perform data quality checks
        for table in self.tables:
            self.log.info(f"Checking data quality for table {table}")

            # Execute a query to count rows in the table
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            # Check if the query returned results
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )

            # Check if the table contains any rows
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows"
                )

            # Log success message if all checks pass
            self.log.info(
                f"Data quality on table {table} check passed with {num_records} records"
            )
