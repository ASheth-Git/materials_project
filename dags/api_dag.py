from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json
import os

# --- CONFIGURATION --
CONNECTION_ID = "my_materials_project_api"
BASE_URL = "https://api.materialsproject.org"
# Save data to a local folder
OUTPUT_PATH = os.path.join(os.environ.get("AIRFLOW_HOME", "."), "data_lake/raw/")

#For DAG
default_args = {
    "owner": "alpesh_sheth",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}



def attempt_connection_to_api(**kwargs):
    try:
        # Fetch the connection object by the ID you created in Airflow UI
        connection  = BaseHook.get_connection(CONNECTION_ID)
        
        # Extract the stored details
        api_url = connection.host       # The URL stored in 'Host' field
        api_password = connection.password  # The password/token stored in 'Password' field
        
        print(f"Successfully retrieved credentials for {CONNECTION_ID}")

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










def fetch_materials_data(**kwargs):
    """
    Fetches Nickelate data using credentials stored in Airflow Connections.
    """
    # 1. Securely get the API Key from Airflow
    try:
        connection = BaseHook.get_connection(CONNECTION_ID)
        api_key = connection.password
        print(f"Successfully retrieved credentials for {CONNECTION_ID}")
    except Exception as e:
        raise ValueError(
            f"Could not find connection '{CONNECTION_ID}'. Did you create it in the UI?"
        )

    # 2. Set up the Request
    params = {
        "elements": "Ni",  # Focusing on Nickel (your PhD topic)
        "_fields": "material_id,formula_pretty,density,band_gap,formation_energy_per_atom",
        "_limit": 50,
    }
    headers = {"X-API-KEY": api_key}

    # 3. Call the API
    print(f"Fetching data from {BASE_URL}...")
    response = requests.get(BASE_URL, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()["data"]
        print(f"Success! Fetched {len(data)} materials.")

        # 4. Save to Disk
        if not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)

        file_name = f"nickelates_{datetime.now().strftime('%Y%m%d')}.json"
        full_path = os.path.join(OUTPUT_PATH, file_name)

        with open(full_path, "w") as f:
            json.dump(data, f)

        print(f"Data saved to: {full_path}")
    else:
        raise Exception(f"API Failed: {response.status_code} - {response.text}")


# # --- DEFINE THE DAG ---
# with DAG(
#     "01_extract_nickelates",
#     default_args=default_args,
#     description="Extracts Nickelate data from Materials Project API",
#     schedule="@daily",  # Run once a day
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
# ) as dag:
#     extract_task = PythonOperator(
#         task_id="extract_from_api",
#         python_callable=fetch_materials_data,c
#     )
