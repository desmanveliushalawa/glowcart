from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'glowcart',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='glowcart_daily_pipeline',
    default_args=default_args,
    description='Pipeline harian GlowCart - generate dan simpan data',
    schedule='0 6 * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['glowcart', 'kafka'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='cd /opt/airflow && timeout 60 python3 /opt/airflow/dags/producer_task.py || true',
    )

    save_data = BashOperator(
        task_id='save_data',
        bash_command='cd /opt/airflow && timeout 30 python3 /opt/airflow/dags/consumer_task.py || true',
    )

    generate_data >> save_data