from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    """
    Custom Airflow operator to stage JSON data from S3 into Redshift tables.
    """
    ui_color = '#358140'  # Sets the UI color for the operator in Airflow's DAG view.

    template_fields = ("s3_key",)  # Enables Jinja templating for the s3_key field.

    # SQL template for the COPY command to stage data into Redshift.
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_file="",
                 *args, **kwargs):
        """
        Initializes the operator with necessary parameters.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param aws_credentials_id: Airflow connection ID for AWS credentials
        :param table: Target Redshift table
        :param s3_bucket: S3 bucket containing the source data
        :param s3_key: Key (path) to the source data in the S3 bucket
        :param log_json_file: JSON path file for log data formatting (optional)
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Executes the operator: deletes existing data in the target table,
        and stages new data from S3 into the Redshift table.
        """
        # Retrieve AWS credentials from the Airflow connection.
        self.log.info("Fetching AWS credentials")
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type="s3")
        credentials = aws_hook.get_credentials()

        # Connect to Redshift using the PostgresHook.
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data in the target table.
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Prepare the S3 path and JSON format option.
        self.log.info("Preparing S3 path and JSON options")
        rendered_key = self.s3_key.format(**context)  # Supports templated keys with execution context.
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"  # Construct the full S3 path.
        json_option = f"s3://{self.s3_bucket}/{self.log_json_file}" if self.log_json_file else "auto"

        # Format the SQL COPY command with the necessary parameters.
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_option
        )

        # Log the details of the COPY command for debugging.
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        self.log.info(f"Executing SQL: {formatted_sql}")

        # Execute the SQL COPY command and handle errors if any occur.
        try:
            redshift.run(formatted_sql)
            self.log.info(f"Success: Data copied to Redshift table {self.table}")
        except Exception as e:
            self.log.error(f"Error copying data to Redshift table {self.table}: {e}")
            raise
