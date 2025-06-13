from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
def extract():
    input_file = '/opt/airflow/dags/retail_data.csv'
    df = pd.read_csv(input_file)
    df.to_csv('/tmp/data_ex.csv', index=False)
    print("Extracted data successfully")

def transform():
    df = pd.read_csv('/tmp/data_ex.csv')
    df['Date'] = df['Date'].str.replace('-', '/', regex=False)
    df['Total_Amount'] = df['Total_Amount'].astype(float)
    df.fillna({'Feedback': 'No Feedback', 'Ratings': 0}, inplace=True)
    df['Tax'] = df['Total_Amount'] * 0.1
    df.to_csv('/tmp/data_tran.csv', index=False)
    print("Transformed data successfully")

def load():
    df = pd.read_csv('/tmp/data_tran.csv')
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    df.to_sql('transactions', engine, if_exists='append', index=False)
    os.remove('/tmp/data_ex.csv')
    os.remove('/tmp/data_tran.csv')
    print("Loaded data successfully")

def send_email():
    try:
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        smtp_user = 'ngo.hieu.a1.nh1@gmail.com' 
        smtp_password = 'grce wmci iwjn ivwa'  
        recipient = 'hieungo.030604@gmail.com'

        # Tạo email
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = recipient
        msg['Subject'] = 'ETL Pipeline Completed'
        body = 'The ETL pipeline for transaction data has completed successfully.'
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipient, msg.as_string())
        print("Email sent successfully")
    except Exception as e:
        raise Exception(f"Failed to send email: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@localmail.com'],
    'email_on_failure': True,
}

with DAG(
    dag_id='etl_transaction_pipeline',
    default_args=default_args,
    description='ETL pipeline for transaction data',
    schedule='@daily',   # ← ĐÃ ĐỔI schedule_interval thành schedule
    start_date=datetime(2025, 6, 13),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email,
    )

    extract_task >> transform_task >> load_task >> email_task
