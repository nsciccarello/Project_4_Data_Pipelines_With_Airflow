from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Custom Airflow operator to load data into a Redshift dimension table.
    """
    ui_color = '#80BD9E'  # Sets the UI color for the operator in Airflow's DAG view.

    # SQL template for inserting data into the dimension table.
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 truncate=False,
                 *args, **kwargs):
        """
        Initializes the operator with required parameters.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param table: Target dimension table in Redshift
        :param sql_stmt: SQL statement used to extract data
        :param truncate: Boolean flag to indicate whether to truncate the table before loading
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate

    def execute(self, context):
        """
        Executes the operator: optionally truncates the table and loads data into it.

        :param context: Airflow execution context (not used in this implementation)
        """
        # Establish connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # If truncate flag is set, clear the table before inserting new data
        if self.truncate:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        # Format the SQL statement to insert data into the dimension table
        self.log.info(f"Loading dimension table '{self.table}' into Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_stmt
        )

        # Execute the formatted SQL statement
        self.log.info(f"Executing SQL: {formatted_sql}")
        redshift.run(formatted_sql)

        # Log success message
        self.log.info(f"Successfully loaded data into dimension table: {self.table}")
