from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests

# 1. Define the function that does the work
def test_my_api_connection(**kwargs):
    # Fetch the connection object by the ID you created in Airflow UI
    conn = BaseHook.get_connection("my_api_connection_id")
    
    # Extract the stored details
    api_url = conn.host       # The URL stored in 'Host' field
    api_password = conn.password  # The password/token stored in 'Password' field
    
    print(f"Attempting to connect to: {api_url}")

    # 2. Establish/Test the actual connection
    # Note: Adjust headers/auth based on your specific API needs (Bearer, Basic, etc.)
    try:
        response = requests.get(
            api_url, 
            headers={"Authorization": f"Bearer {api_password}"}, 
            timeout=10
        )
        
        # 3. Check and Print
        if response.status_code == 200:
            print("Connection Successful!")
        else:
            print(f"Connection Failed. Status Code: {response.status_code}")
            # Raise error to mark task as failed in Airflow
            response.raise_for_status() 
            
    except Exception as e:
        print(f"Connection Error: {e}")
        raise e

# 4. Define the DAG
with DAG(
    dag_id="verify_api_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    check_conn_task = PythonOperator(
        task_id="check_api_status",
        python_callable=test_my_api_connection
    )