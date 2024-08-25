from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define DAG Arguments
default_args = {
    'owner': 'Yudi_Arrasyid',
    'start_date': datetime(2024, 8, 25),
    'email': ['yudiarrasyid12@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# Task 1: Unzip Data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/zara/airflow/dags/finalassignment/tolldata.tgz -C /home/zara/airflow/dags/finalassignment/staging',
    dag=dag,
)

# Task 2: Extract Data From CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1,2,3,4 /home/zara/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/zara/airflow/dags/finalassignment/staging/csv_data.csv",
    dag=dag,
)

# Task 3: Extract Data From TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5,6,7 /home/zara/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/zara/airflow/dags/finalassignment/staging/tsv_data.csv",
    dag=dag,
)

# Task 4: Extract Data From Fixed Width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c10-19,20-29 /home/zara/airflow/dags/finalassignment/staging/payment-data.txt > /home/zara/airflow/dags/finalassignment/staging/fixed_width_data.csv",
    dag=dag,
)

# Task 5: Consolidate Data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d',' /home/zara/airflow/dags/finalassignment/staging/csv_data.csv /home/zara/airflow/dags/finalassignment/staging/tsv_data.csv /home/zara/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/zara/airflow/dags/finalassignment/staging/extracted_data.csv",
    dag=dag,
)

# Task 6: Transform Data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk -F',' '{OFS=\",\"; $4=toupper($4); print}' /home/zara/airflow/dags/finalassignment/staging/extracted_data.csv > /home/zara/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,
)

# Define Task Pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

