from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Custom Airflow operator to load data into a Redshift fact table.
    """
    ui_color = '#F98866'  # Sets the UI color for the operator in Airflow's DAG view.

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):
        """
        Initializes the operator with required parameters.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param table: Target fact table in Redshift
        :param sql_query: SQL query used to extract and load data
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Executes the operator: loads data into the specified fact table.

        :param context: Airflow execution context (not used in this implementation)
        """
        # Establish connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Log the loading process
        self.log.info(f"Loading data into Redshift fact table: {self.table}")

        # Construct and execute the SQL command to load data into the fact table
        insert_sql = f"INSERT INTO {self.table} {self.sql_query}"
        self.log.info(f"Executing SQL: {insert_sql}")
        redshift.run(insert_sql)

        # Log success message after data load
        self.log.info(f"Successfully loaded data into fact table: {self.table}")
