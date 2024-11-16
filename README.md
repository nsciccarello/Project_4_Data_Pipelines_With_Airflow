# Project 4: Data Pipelines With Airflow

## Project Overview:

This project demonstrates how to build data pipelines using Apache Airflow for a music streaming company, Sparkify. The goal is to automate and monitor the ETL (Extract, Transform, Load) processes for loading data from S3 into an Amazon Redshift data warehouse, transforming the data, and running data quality checks.

The project showcases the following features:
- Dynamic and reusable pipelines
- Custom Airflow operators
- Staging, transforming, and quality checking data
- Backfilling capabilities

## Project Structure:
```
├── dags/
│   ├── final_project.py                  # Main DAG definition for ETL pipeline
│   ├── create_tables_dag.py              # DAG to create Redshift tables
├── plugins/
│   ├── final_project_operators/
│   │   ├── stage_redshift.py             # Custom operator to stage data into Redshift
│   │   ├── load_fact.py                  # Custom operator to load data into fact table
│   │   ├── load_dimension.py             # Custom operator to load data into dimension tables
│   │   ├── data_quality.py               # Custom operator for data quality checks
│   └── udacity/
│       ├── common/
│       │   └── final_project_sql_statements.py # SQL queries for transformations
├── sql/
│   ├── create_tables.sql                 # SQL script to create Redshift tables
│   ├── log_json_path.json                # JSON path file for parsing log data
├── README.md                             # Project documentation (this file)
```
## Datasets:

The project uses two datasets stored in S3:
1. **Log Data:**
	- **Path:** s3://udacity-dend/log_data
	- **Description:** JSON logs detailing user activity in the Sparkify app.
2. **Song Data:**
    - **Path:** s3://udacity-dend/song_data
    - **Description:** JSON metadata about the songs available in the app.

## Workflow:

1. **Staging Tables:**
   - Data is loaded from S3 into two staging tables (staging_events and staging_songs) in Redshift.
2. **Fact Table:**
   - Data from staging tables is transformed and inserted into the songplays fact table.
3. **Dimension Tables:**
   - Data is further transformed and inserted into the users, songs, artists, and time dimension tables.
4. **Data Quality Checks:**
   - Ensures that the fact and dimension tables have valid data (e.g., no empty rows, no null primary keys).

## Custom Operators:

The project uses four custom operators. They are the StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, and the DataQualityOperator.

1. **StageToRedshiftOperator:** Copies data from S3 to Redshift staging tables.
2. **LoadFactOperator:** Inserts transformed data into the fact table.
3. **LoadDimensionOperator:** Inserts transformed data into dimension tables and supports truncate-insert mode.
4. **DataQualityOperator:** Runs SQL-based checks to validate data integrity.

## Steps to Run the Project:

1. **Setup AWS Resources:**
   - Create an S3 bucket and copy the data:
     ```
     aws s3 cp s3://udacity-dend/log_data/ s3://data-pipelines-project/log_data/ --recursive
     aws s3 cp s3://udacity-dend/song_data/ s3://data-pipelines-project/song_data/ --recursive
     ```
   - Set up an Amazon Redshift cluster and configure connections in Airflow.
2. **Airflow Configuration:**
   - Add AWS and Redshift connections in Airflow:
     - **AWS:** Connection ID aws_credentials.
     - **Redshift:** Connection ID redshift.
3. **Run the Create Tables DAG:**
   - Use the create_tables_dag to create the necessary tables in Redshift.
4. **Run the Main DAG:**
   - Trigger the final_project_dag in Airflow to execute the ETL pipeline.
5. **Verify the Data:**
   - Use Redshift Query Editor or a database client to inspect the loaded tables.

## SQL Queries:

All transformation queries are defined in final_project_sql_statements.py. These include:
- **songplay_table_insert:** Inserts data into the songplays fact table.
- **user_table_insert:** Inserts data into the users dimension table.
- **song_table_insert:** Inserts data into the songs dimension table.
- **artist_table_insert:** Inserts data into the artists dimension table.
- **time_table_insert:** Inserts data into the time dimension table.

## Airflow DAGs:

1. **Create Tables DAG:**
   - Ensures the Redshift tables are created before running the ETL pipeline.
   - **Tasks:**
     - Begin_execution: Marks the start of the DAG.
     - Create_tables: Executes create_tables.sql in Redshift.
     - Stop_execution: Marks the end of the DAG.
2. **Final Project DAG:**
   - Executes the full ETL workflow.
   - Task Flow:
     - Begin_execution → Stage_events, Stage_songs → Load_songplays_fact_table → Load Dimension Tables → Run_data_quality_checks → Stop_execution

## Testing:
- Verified data integrity by running queries on the Redshift tables.
- Used Airflow task logs to debug issues during development.

## Conclusion:

This project demonstrates the power of Apache Airflow in building, automating, and monitoring ETL pipelines for a real-world scenario. By leveraging custom operators, reusable SQL queries, and modular DAGs, we successfully loaded, transformed, and validated data in Amazon Redshift to create a robust data warehouse for Sparkify.

The structured pipeline ensures that:
	1. **Scalability:** The solution can handle larger datasets as the company grows.
	2. **Reusability:** Custom operators and SQL queries can be adapted for other workflows.
	3. **Data Integrity:** Data quality checks ensure reliable analytics.

This project highlights the importance of automation and monitoring in modern data engineering workflows and provides a scalable foundation for future enhancements, such as real-time processing or integrating additional data sources.
