import json
import os
from datetime import datetime
from kafka import KafkaConsumer

os.makedirs('/opt/airflow/data/raw/orders', exist_ok=True)
os.makedirs('/opt/airflow/data/raw/user-events', exist_ok=True)
os.makedirs('/opt/airflow/data/raw/payments', exist_ok=True)

def save_and_consume(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id=f'airflow-{topic}-consumer',
        consumer_timeout_ms=10000
    )
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = f'/opt/airflow/data/raw/{topic}/{date_str}.jsonl'
    count = 0
    with open(filepath, 'a') as f:
        for message in consumer:
            f.write(json.dumps(message.value) + '\n')
            count += 1
    print(f"[{topic}] {count} records disimpan ke {filepath}")
    consumer.close()

for topic in ['orders', 'user-events', 'payments']:
    save_and_consume(topic)