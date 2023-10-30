from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import uuid
import json
import time

# Define the number of login iterations as a parameter
from airflow.models import Variable


# Task 1: Generate a user via an API
def generate_user(api_url, **kwargs):
    unique_username = f"user_{str(uuid.uuid4())[:8]}"  # Generate a unique username
    unique_password = f"password_{str(uuid.uuid4())[:8]}"  # Generate a unique password
    kwargs['ti'].xcom_push(key='username', value=unique_username)
    kwargs['ti'].xcom_push(key='password', value=unique_password)
    login_data = {
        "password": unique_password, 
        "policies": ["default","test"],
    }
    headers = {
        "X-Vault-Token": vault_token,  # Replace with your actual token value
    }
    response = requests.put(api_url + "/v1/auth/userpass/users/" + unique_username, json=login_data, headers=headers)
    print("Username: " + unique_username)
    print("URL: " + response.url)
    print("Status: " + str(response.status_code))
    print("Body: " + response.text)

# Task 2: Login as the user (multiple times)
def login_as_user(api_url, num_iterations, **kwargs):
    user_id = kwargs['ti'].xcom_pull(task_ids='generate_user_task', key='username')
    user_password = kwargs['ti'].xcom_pull(task_ids='generate_user_task', key='password')
    headers = {
        "X-Vault-Token": vault_token,  # Replace with your actual token value
    }
    if user_id:
        for _ in range(num_iterations):
            login_data = {"password": user_password}
            response = requests.put(api_url + "/v1/auth/userpass/login/" + user_id, json=login_data, headers=headers)
            print("Body: " + response.text)
            if response.status_code == 200:
                print(f"Login successful for user {user_id}")
                response_data = json.loads(response.text)
                client_token = response_data['auth']['client_token']
                print("Client Token:", client_token)
                kv_data = {
                    "data":{"owner":user_id},
                    "options":{}
                }
                kv_headers = {
                    "X-Vault-Token": client_token,  # Replace with your actual token value
                }
                response = requests.put(api_url + "/v1/secret/data/test", json=kv_data, headers=kv_headers)
                if response.status_code == 200:
                    print(f"KV secret created for user {user_id}")
                else:
                    print(f"Failed to create KV secret for user {user_id}")
            else:
                print(f"Login failed for user {user_id}")
    else:
        print("User generation failed.")

# Task 3: Get a client count
def get_client_count(api_url, **kwargs):
    headers = {
        "X-Vault-Token": vault_token,  # Replace with your actual token value
    }
    # Get the current epoch time
    current_epoch_time = int(time.time())

    # Calculate the epoch time for 2 days ago (48 hours ago)
    two_days_ago_epoch_time = current_epoch_time - (2 * 24 * 3600)

    response = requests.get(api_url + "/v1/sys/internal/counters/activity?start_time=" + str(two_days_ago_epoch_time) + "&end_time=" + str(current_epoch_time), headers=headers)
    if response.status_code == 200:
        print("Body: " + response.text)
        response_data = json.loads(response.text)
        total_clients = response_data.get("data", {}).get("total", {}).get("clients")
        if total_clients is not None:
            print(f"Total Clients: {total_clients}")
        else:
            print("Total Clients not found in the JSON response")
    else:
        print("Failed to get client count")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'vault_entity_dag',
    default_args=default_args,
    description='DAG to login as admin, generate a user, log in multiple times, and get a client count',
    schedule_interval=None,  # This DAG is triggered manually (adhoc)
    catchup=False,
)

# Define the API URL as a parameter
api_url = Variable.get("api_url", default_var="http://host.docker.internal:8200")
vault_token = Variable.get("vault_token", default_var="hvs.6EGo6BeynAIJQeTu0VdBb7mh")

# Task 1: Generate a user
generate_user_task = PythonOperator(
    task_id='generate_user_task',
    python_callable=generate_user,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 2: Login as the user (multiple times)
login_task = PythonOperator(
    task_id='login_task',
    python_callable=login_as_user,
    provide_context=True,
    op_args=[api_url, 1],  # Provide the URL and num_iterations as arguments
    dag=dag,
)

# Task 3: Get a client count
get_client_count_task = PythonOperator(
    task_id='get_client_count_task',
    python_callable=get_client_count,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Set task dependencies
generate_user_task >> login_task >> get_client_count_task

if __name__ == "__main__":
    dag.cli()
