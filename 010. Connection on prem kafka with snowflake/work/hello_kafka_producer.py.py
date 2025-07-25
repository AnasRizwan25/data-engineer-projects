from kafka import KafkaConsumer
from snowflake.connector import connect
from dotenv import load_dotenv
import os, json

# Load env variables
load_dotenv()

# Kafka Consumer
consumer = KafkaConsumer(
    '{topic_name}',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Snowflake connection
conn = connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)
cursor = conn.cursor()

# Insert loop
for message in consumer:
    data = message.value
    print("Received:", data)
    cursor.execute(
        "INSERT INTO {table_name} (value) SELECT PARSE_JSON(%s)", 
        [json.dumps(data)]
    )
