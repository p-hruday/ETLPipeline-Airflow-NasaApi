from airflow.sdk import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timezone
import json


## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),  # days_ago() removed in Airflow 3
    schedule='@daily',                                      # schedule_interval renamed to schedule
    catchup=False
) as dag:

    ## Step 1: Create the table if it doesn't exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    ## Step 2: Extract NASA APOD data
    # SimpleHttpOperator replaced with HttpHook inside a @task —
    # avoids Jinja template issues with conn.extra_dejson in Airflow 3
    @task
    def extract_apod():
        import requests

        conn_id = 'nasa_api'
        http_hook = HttpHook(http_conn_id=conn_id, method='GET')

        conn = http_hook.get_connection(conn_id)
        api_key = conn.extra_dejson.get('api_key')

        if not api_key:
            raise ValueError("No api_key found in nasa_api connection extras")

        # Build the full URL manually to guarantee the key is in the query string
        base_url = f"{conn.host.rstrip('/')}/planetary/apod"
        response = requests.get(base_url, params={"api_key": api_key})
        response.raise_for_status()
        return response.json()

    ## Step 3: Transform the data
    @task
    def transform_apod_data(response):
        # Guard against XCom returning a JSON string instead of a dict
        if isinstance(response, str):
            response = json.loads(response)

        return {
            'title':       response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url':         response.get('url', ''),
            'date':        response.get('date', ''),
            'media_type':  response.get('media_type', '')
        }

    ## Step 4: Load data into PostgreSQL
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    ## Step 5: Define task dependencies
    create_table_task  = create_table()
    extract_task       = extract_apod()
    transformed_data   = transform_apod_data(extract_task)

    create_table_task >> extract_task >> transformed_data
    load_data_to_postgres(transformed_data)