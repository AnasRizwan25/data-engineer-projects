from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3

# --- AWS S3 Configuration ---
AWS_ACCESS_KEY_ID = "---"
AWS_SECRET_ACCESS_KEY = "---"
AWS_REGION = "---"
S3_BUCKET = "---"

# --- DAG Default Arguments ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id='---',
    default_args=default_args,
    start_date=datetime(2025, 7, 23),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    def fetch_weather_data(**kwargs):
        url = "https://wttr.in/?format=j1"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        kwargs['ti'].xcom_push(key='raw_weather_data', value=data)

    def transform_weather_data(**kwargs):
        data = kwargs['ti'].xcom_pull(key='raw_weather_data', task_ids='fetch_weather')
        current = data['current_condition'][0]
        transformed = {
            'temperature_C': current['temp_C'],
            'temperature_F': current['temp_F'],
            'humidity': current['humidity'],
            'weather_desc': current['weatherDesc'][0]['value'],
            'observation_time': current['observation_time']
        }
        kwargs['ti'].xcom_push(key='transformed_weather', value=transformed)

    def upload_to_s3(**kwargs):
        transformed = kwargs['ti'].xcom_pull(key='transformed_weather', task_ids='transform_weather_data')
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")

        s3_key = f"weather-data/{year}/{month}/{day}/{hour}/weather.json"

        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3 = session.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(transformed),
            ContentType='application/json'
        )
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")

    # Tasks
    fetch_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data
    )

    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    fetch_task >> transform_task >> upload_task
