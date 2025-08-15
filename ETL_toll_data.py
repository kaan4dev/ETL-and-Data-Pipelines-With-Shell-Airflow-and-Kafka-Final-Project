from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'kaan',
    'start_date': datetime.now(),
    'email': ['kaan@ex.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
    catchup=False
) as dag:

    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command='tar -xvf /home/project/tolldata.tgz -C /home/project/staging/'
    )

    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command='cut -d"," -f1,2,3,4 /home/project/staging/vehicle-data.csv > /home/project/staging/csv_data.csv'
    )

    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command='cut -f5,6,7 /home/project/staging/tollplaza-data.tsv | tr "\\t" "," > /home/project/staging/tsv_data.csv'
    )

    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command='cut -c59-61,63-67 /home/project/staging/payment-data.txt | tr " " "," > /home/project/staging/fixed_width_data.csv'
    )

    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command='paste -d"," /home/project/staging/csv_data.csv /home/project/staging/tsv_data.csv /home/project/staging/fixed_width_data.csv > /home/project/staging/extracted_data.csv'
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=(
            'cut -d"," -f1,2,3,4 --complement /home/project/staging/extracted_data.csv > /home/project/staging/temp.csv && '
            'paste -d"," <(cut -d"," -f1-3 /home/project/staging/extracted_data.csv) '
            '<(cut -d"," -f4 /home/project/staging/extracted_data.csv | tr "[:lower:]" "[:upper:]") '
            '/home/project/staging/temp.csv > /home/project/staging/transformed_data.csv'
        )
    )

    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
