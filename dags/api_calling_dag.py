from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json
import os
from mp_api.client import MPRester

# --- CONFIGURATION --
CONNECTION_ID = "my_materials_project_api"
BASE_URL = "https://api.materialsproject.org"
# Save data to a local folder
OUTPUT_PATH = os.path.join(os.environ.get("AIRFLOW_HOME", "."), "data_lake/raw/")

# For DAG
default_args = {
    "owner": "alpesh_sheth",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def test_my_api_connection(**kwargs):

    # Fetch the connection object by the ID you created in Airflow UI
    conn = BaseHook.get_connection(CONNECTION_ID)

    # Extract the stored details
    api_url = conn.host  # The URL stored in 'Host' field

    api_key = conn.password  # The password/token stored in 'Password' field

    print(f"Attempting to connect to: {api_url}")

    # 2. Establish/Test the actual connection
    try:
        response = requests.get(
            api_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10
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


def fetch_materials_data(**kwargs):

    # Fetch the connection object by the ID you created in Airflow UI
    conn = BaseHook.get_connection(CONNECTION_ID)

    # Extract the stored details
    api_url = conn.host  # The URL stored in 'Host' field

    api_key = conn.password  # The password/token stored in 'Password' field

    if not api_key:
        raise ValueError("No API key found in connection 'my_materials_project_api'")

    else:
        print(f"Authenticating with MPRester...")

    with MPRester(api_key) as mpr:
        docs = mpr.materials.summary.search(
            material_ids=["mp-149", "mp-13", "mp-22526"]
        )
        print(f"Found {len(docs)} documents.")

    # 3. Setup Output Directory
    # 3a. Get the Airflow Home directory (usually /opt/airflow)
    # airflow_home =

    # 3b. Define path inside the DAGS folder so it is visible to you
    # This will save to: /opt/airflow/dags/output/
    output_dir = os.path.join(os.environ.get("AIRFLOW_HOME", "."), "data_lake/output/")

    os.makedirs(output_dir, exist_ok=True)
    # Create folder if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # 4. Create Filename (Time of run: YYYYMMDD_HHMM)
    # %Y = Year, %m = Month, %d = Day
    # %H = Hour (24-hour clock), %M = Minute
    current_time = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"output_{current_time}.txt"
    file_path = os.path.join(output_dir, filename)

    # 5. Write to File
    print(f"Writing results to: {file_path}")

    with open(file_path, "w") as f:
        f.write(f"Run Time: {current_time}\n")
        f.write("-" * 30 + "\n")

        for doc in docs:
            # Prepare the string line
            line = f"ID: {doc.material_id} | Formula: {doc.formula_pretty}\n"

            # Write to file
            f.write(line)

            # (Optional) Still print to Airflow logs if you want
            print(f"Saved: {line.strip()}")

    return file_path

    # results = []
    # for doc in docs:
    #     # Extract specific fields
    #     mpid = doc.material_id
    #     formula = doc.formula_pretty

    #     # Print for logs
    #     print(f"--- Processed Material ---")
    #     print(f"ID: {mpid}")
    #     print(f"Formula: {formula}")

    #     # Append to list for potential return/storage
    #     results.append({"material_id": str(mpid), "formula": formula})

    # # 4. (Optional) Push to XCom so other tasks can use it
    # return print(results)


with DAG(
    dag_id="02_materials_project_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    # Task 1: Check Connection (Using your logic)
    t1_test_conn = PythonOperator(
        task_id="test_connection_task", python_callable=test_my_api_connection
    )

    # Task 2: Fetch Data using MPRester
    t2_fetch_data = PythonOperator(
        task_id="fetch_materials_data", python_callable=fetch_materials_data
    )

    # Set dependency: Run test first, then fetch data
    t1_test_conn >> t2_fetch_data
