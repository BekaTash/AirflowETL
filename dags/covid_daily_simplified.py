from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, VARCHAR, FLOAT, BIGINT, DATETIME, TIMESTAMP, Table, MetaData, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import insert
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sqlalchemy
import requests
import logging
import os
import pandas as pd
import psycopg2

# Defining the last date for which data should be retrieved
LAST_DATA_DATE = datetime(2022, 12, 1)

def create_aggregated_covid_table():
    # Define the SQL statement to create the table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS aggregated_covid_data (
        Country_Region VARCHAR(255),
        Last_Update TIMESTAMP,
        Avg_Lat FLOAT,
        Avg_Long FLOAT,
        Total_Confirmed BIGINT,
        Total_Deaths BIGINT,
        Total_Recovered BIGINT,
        Avg_Incident_Rate FLOAT,
        Avg_Case_Fatality_Ratio FLOAT,
        CONSTRAINT aggregated_covid_data_pkey PRIMARY KEY (Country_Region, Last_Update)
    );
    """
    #connecttion:
    engine = create_engine('postgresql+psycopg2://postgres:postgres@host.docker.internal:5433/postgres')
    
    # Execute the SQL statement
    with engine.connect() as conn:
        conn.execute(create_table_sql)
    
    return 'aggregated_covid_data'

def download_covid_data(**kwargs):
    execution_date = kwargs['execution_date']
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d')
    formatted_date = execution_date.strftime('%m-%d-%Y')  # Format date to MM-DD-YYYY
    url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{formatted_date}.csv"
    logging.info(f'Sending get request to COVID repo by URL: {url}')
    response = requests.get(url)
    if response.status_code == 200:
        save_dir = "/opt/airflow/dags/files/"
        os.makedirs(save_dir, exist_ok=True)
        save_path = f"{save_dir}{formatted_date}.csv"
        logging.info(f'Successfully got data. Saving to file: {save_path}')
        with open(save_path, 'w') as f:
            f.write(response.text)
    else:
        raise ValueError(f'Unable to get data from URL: {url}')
    
def aggregate_covid_data(file_path):
    df = pd.read_csv(file_path)
    # Airflow error fix, ensuring Last_Update is in datetime format
    df['Last_Update'] = pd.to_datetime(df['Last_Update'], errors='coerce')

    # Fill missing values before aggregation
    df['Lat'] = df['Lat'].fillna(df['Lat'].mean())
    df['Long_'] = df['Long_'].fillna(df['Long_'].mean())
    df['Incident_Rate'] = df['Incident_Rate'].fillna(df['Incident_Rate'].mean())
    df['Case_Fatality_Ratio'] = df['Case_Fatality_Ratio'].fillna(df['Case_Fatality_Ratio'].mean())

    aggregated_df = df.groupby('Country_Region').agg({
    'Last_Update': 'max',
    'Lat': 'mean',
    'Long_': 'mean',
    'Confirmed': 'sum',
    'Deaths': 'sum',
    'Recovered': 'sum',
    'Incident_Rate': 'mean',
    'Case_Fatality_Ratio': 'mean'
}).reset_index().rename(columns={
    'Lat': 'Avg_Lat',
    'Long_': 'Avg_Long',
    'Confirmed': 'Total_Confirmed',
    'Deaths': 'Total_Deaths',
    'Recovered': 'Total_Recovered',
    'Incident_Rate': 'Avg_Incident_Rate',
    'Case_Fatality_Ratio': 'Avg_Case_Fatality_Ratio'
})
    aggregated_df['Last_Update'] = aggregated_df['Last_Update'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    
    aggregated_file_path = file_path.replace('.csv', '_aggregated.csv')
    aggregated_df.to_csv(aggregated_file_path, index=False)
    return aggregated_file_path
    
def clean_covid_data(file_path):
    """
    Cleans the COVID data by handling null values and ensuring data consistency.
    - Replaces nulls in 'FIPS' and 'Admin2' with a placeholder value (e.g., 'Unknown').
    - Drops 'Recovered' and 'Active' columns as they are entirely null.
    """
    df = pd.read_csv(file_path)
     #Airflow error fix    
    # Check for 'Lat' and 'Long_' columns and fill missing values if they exist
    if 'Lat' in df.columns:
        df['Lat'] = df['Lat'].fillna(0)  # Fill missing values with 0 for 'Lat'
    if 'Long_' in df.columns:
        df['Long_'] = df['Long_'].fillna(0)  # Fill missing values with 0 for 'Long_'

    # Check for 'Incident_Rate' and 'Case_Fatality_Ratio' columns and fill missing values if they exist
    if 'Incident_Rate' in df.columns:
        df['Incident_Rate'] = df['Incident_Rate'].fillna(df['Incident_Rate'].mean())  # Fill missing values with the mean
    if 'Case_Fatality_Ratio' in df.columns:
        df['Case_Fatality_Ratio'] = df['Case_Fatality_Ratio'].fillna(df['Case_Fatality_Ratio'].mean())  # Fill missing values with the mean

    # Drop 'Recovered' and 'Active' columns if they are present
    df.drop(columns=['Recovered', 'Active'], inplace=True, errors='ignore')

    # Save the cleaned data back to the same file
    df.to_csv(file_path, index=False)

def validate_covid_data(file_path):
    """
    Validates the COVID data to ensure it meets certain criteria before processing.
    This could include checks like ensuring all expected columns are present and non-empty.
    """
    df = pd.read_csv(file_path)
    required_columns = [
    'Country_Region', 'Last_Update', 'Avg_Lat', 'Avg_Long',
    'Total_Confirmed', 'Total_Deaths', 'Total_Recovered',
    'Avg_Incident_Rate', 'Avg_Case_Fatality_Ratio'
]
# Check for missing required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"
    
    # Detailed logging for null values
    for col in required_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logging.warning(f"Column '{col}' contains {null_count} null values")
    
    # Assert no null values in required columns
    assert df.dropna(subset=required_columns).shape[0] == df.shape[0], "Null values found in required columns"

def insert_data_into_postgres(table_name, execution_date, **kwargs):
    # Convert execution_date from string to datetime object if necessary
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d')

    # Database connection settings
    DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@host.docker.internal:5433/postgres'

    # Create a database engine
    engine = create_engine(DATABASE_URI)

    # Reflect the table structure from the database
    metadata = MetaData(bind=engine)
    aggregated_covid_data = Table(table_name, metadata, autoload=True)

    # Generate file path for the given execution date
    formatted_date = execution_date.strftime('%m-%d-%Y')
    file_path = f"/opt/airflow/dags/files/{formatted_date}_aggregated.csv"

    # Check if the file exists before processing
    if os.path.exists(file_path):
        # Read CSV file
        df = pd.read_csv(file_path)

        # Data preprocessing (assuming it's done in the previous tasks)
            # Data preprocessing
        df['Last_Update'] = pd.to_datetime(df['Last_Update']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.drop_duplicates(subset=['Country_Region', 'Last_Update'], inplace=True)

        # Connect to the database
        with engine.connect() as conn:
            trans = conn.begin()  # begin new transaction
            try:
                for _, row in df.iterrows():
                    # Create an insert statement for upsert
                    stmt = pg_insert(aggregated_covid_data).values(
                        country_region=row['Country_Region'],
                        last_update=row['Last_Update'],
                        avg_lat=row['Avg_Lat'],
                        avg_long=row['Avg_Long'],
                        total_confirmed=row['Total_Confirmed'],
                        total_deaths=row['Total_Deaths'],
                        total_recovered=row['Total_Recovered'],
                        avg_incident_rate=row['Avg_Incident_Rate'],
                        avg_case_fatality_ratio=row['Avg_Case_Fatality_Ratio']
                    )
                    upsert_stmt = stmt.on_conflict_do_update(
                        constraint='aggregated_covid_data_pkey',
                        set_={
                            'avg_lat': stmt.excluded.avg_lat,
                            'avg_long': stmt.excluded.avg_long,
                            'total_confirmed': stmt.excluded.total_confirmed,
                            'total_deaths': stmt.excluded.total_deaths,
                            'total_recovered': stmt.excluded.total_recovered,
                            'avg_incident_rate': stmt.excluded.avg_incident_rate,
                            'avg_case_fatality_ratio': stmt.excluded.avg_case_fatality_ratio
                        }
                    )
                    # Execute the statement
                    conn.execute(upsert_stmt)
                trans.commit()  # commit the transaction only if all statements executed without error
            except Exception as e:
                trans.rollback()  # rollback the transaction on error
                raise e
        print(f"Data from {file_path} has been processed.")
    else:
        print(f"File {file_path} does not exist.")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 1, 1),
    'end_date': datetime(2021, 1, 10),
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
}

with DAG(
    'covid_data_to_postgres',
    default_args=default_args,
    description='DAG for downloading COVID data and storing in PostgreSQL',
    schedule_interval='@daily',
    catchup=True,
) as dag:

    create_aggregated_table_task = PythonOperator(
        task_id='create_aggregated_table_task',
        python_callable=create_aggregated_covid_table,
        dag=dag,
    )
    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',
        http_conn_id='http_default',
        endpoint='CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{{ execution_date.strftime("%m-%d-%Y") }}.csv',
        poke_interval=5,
        timeout=20,
        dag=dag,
    )
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_covid_data,
        provide_context=True,
        dag=dag,
    )
    # New data cleaning task
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_covid_data,
        op_kwargs={'file_path': '/opt/airflow/dags/files/{{ execution_date.strftime("%m-%d-%Y") }}_aggregated.csv'},
        dag=dag,
    )
    aggregate_data =PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_covid_data,
        op_kwargs={'file_path': '/opt/airflow/dags/files/{{ execution_date.strftime("%m-%d-%Y") }}.csv'},
    )
    # New data validation task
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_covid_data,
        op_kwargs={'file_path': '/opt/airflow/dags/files/{{ execution_date.strftime("%m-%d-%Y") }}_aggregated.csv'},
    )
    insert_data_into_postgres = PythonOperator(
        task_id='insert_data_into_postgres',
        python_callable=insert_data_into_postgres,
        provide_context=True,
        op_kwargs={'table_name': 'aggregated_covid_data','execution_date': '{{ execution_date.strftime("%Y-%m-%d") }}'}, #adding execution date to fix error 
        dag=dag,
    )
    create_aggregated_table_task >> check_if_data_available >> download_data >> aggregate_data >> clean_data >> validate_data >> insert_data_into_postgres
