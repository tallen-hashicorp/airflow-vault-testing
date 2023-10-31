from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import uuid
import json
import time

# Define the number of login iterations as a parameter
from airflow.models import Variable

# Define the API URL as a parameter
api_url = Variable.get("api_url", default_var="http://host.docker.internal:8200")
vault_token = Variable.get("vault_token", default_var="hvs")
num_users = Variable.get("num_users", default_var=100)

def generate_user(api_url, **kwargs):
    headers = {
        "X-Vault-Token": vault_token,  # Replace with your actual token value
    }
    users_data = []
    for _ in range(int(num_users)):
        unique_username = f"user_{str(uuid.uuid4())[:8]}"  # Generate a unique username
        unique_password = f"password_{str(uuid.uuid4())[:8]}"  # Generate a unique password

        create_auth_data = {
            "type":"userpass",
        }
        create_auth_response = requests.post(api_url + "/v1/sys/auth/" + unique_username, json=create_auth_data, headers=headers)
        print("create_auth_response: " + str(create_auth_response.status_code))

        auth_response = requests.get(api_url + "/v1/sys/auth", headers=headers)
        auth_data = json.loads(auth_response.text)
        accessor_value = auth_data.get(unique_username + "/").get("accessor", None)
        users_data.append({"username": unique_username, "password": unique_password, "accessor_value": accessor_value})

        login_data = {
            "password": unique_password, 
            "policies": ["default","test"],
        }
        response = requests.put(api_url + "/v1/auth/" + unique_username + "/users/" + unique_username, json=login_data, headers=headers)
        print("Username: " + unique_username)
        print("URL: " + response.url)
        print("Status: " + str(response.status_code))
        print("Body: " + response.text)
    kwargs['ti'].xcom_push(key='user_data', value=users_data)
    return users_data


def login_as_user(api_url, **kwargs):
    user_data = kwargs['ti'].xcom_pull(task_ids='generate_user_task', key='user_data')
    for user in user_data:
        user_id = user['username']
        user_password = user['password']
        print(user_id)
        print(user_password)
        headers = {
            "X-Vault-Token": vault_token,  # Replace with your actual token value
        }
        if user_id:
            login_data = {"password": user_password}
            response = requests.put(api_url + "/v1/auth/" + user_id + "/login/" + user_id, json=login_data, headers=headers)
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
        kwargs['ti'].xcom_push(key='total_clients', value=total_clients)
        if total_clients is not None:
            print(f"Total Clients: {total_clients}")
        else:
            print("Total Clients not found in the JSON response")
    else:
        print("Failed to get client count")

def create_entity(api_url, **kwargs):
    headers = {
        "X-Vault-Token": vault_token,  # Replace with your actual token value
    }
    enitity_name = f"entity_{str(uuid.uuid4())[:8]}"
    data = {
        "metadata": ["organization=ACME Inc.","team=QA"],
        "name": enitity_name
    }
    enitity_response = requests.put(api_url + "/v1/identity/entity", headers=headers, json=data)
    enitity_data = json.loads(enitity_response.text)
    print("enitity_data:" + str(enitity_data)) 
    entity_id = enitity_data.get('data').get('id')
    print("Entity ID:" + entity_id) 
    kwargs['ti'].xcom_push(key='entity_id', value=entity_id)

def alias_users(api_url, **kwargs):
    headers = {
        "X-Vault-Token": vault_token,  # Replace with your actual token value
    }
    user_data = kwargs['ti'].xcom_pull(task_ids='generate_user_task', key='user_data')
    for user in user_data:
        data = {
            "canonical_id": kwargs['ti'].xcom_pull(task_ids='create_entity_task', key='entity_id'),
            "mount_accessor": user['accessor_value'],
            "name": user['username']
        }
        print("create_alias_data:" + str(data))
        alias_response = requests.put(api_url + "/v1/identity/entity-alias", headers=headers, json=data)
        alias_data = json.loads(alias_response.text)
        print("alias_data:" + str(alias_data))

def print_output(**kwargs):
    print("Users Created: " + str(num_users))
    print("Client Before Starting: " + str(kwargs['ti'].xcom_pull(task_ids='get_starting_count_task', key='total_clients')))
    print("Client After users login after created: " + str(kwargs['ti'].xcom_pull(task_ids='get_client_count_task', key='total_clients')))
    print("Client After users login after they have alias: " + str(kwargs['ti'].xcom_pull(task_ids='get_client_count_after_alias', key='total_clients')))
    print("Client After users login after they have alias and logged in again: " + str(kwargs['ti'].xcom_pull(task_ids='get_client_count_after_alias_login', key='total_clients')))

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

# Task 1: Get a client count at start
get_starting_count_task = PythonOperator(
    task_id='get_starting_count_task',
    python_callable=get_client_count,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 2: Generate a user
generate_user_task = PythonOperator(
    task_id='generate_user_task',
    python_callable=generate_user,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 3: Login as the user (multiple times)
login_task = PythonOperator(
    task_id=f'login_task',
    python_callable=login_as_user,
    provide_context=True,
    op_args=[api_url],  # Provide the URL and num_iterations as arguments
    dag=dag,
)

# Task 4: Get a client count
get_client_count_task = PythonOperator(
    task_id='get_client_count_task',
    python_callable=get_client_count,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 5: Create Entity
create_entity_task = PythonOperator(
    task_id='create_entity_task',
    python_callable=create_entity,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 6: Alias all the users
alias_users_task = PythonOperator(
    task_id='alias_users_task',
    python_callable=alias_users,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 7: Get a client count
get_client_count_after_alias_task = PythonOperator(
    task_id='get_client_count_after_alias',
    python_callable=get_client_count,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 8: Login as the user (multiple times)
login_after_alias_task = PythonOperator(
    task_id=f'login_after_alias_task',
    python_callable=login_as_user,
    provide_context=True,
    op_args=[api_url],  # Provide the URL and num_iterations as arguments
    dag=dag,
)

# Task 9: Get a client count
get_client_count_after_alias_login = PythonOperator(
    task_id='get_client_count_after_alias_login',
    python_callable=get_client_count,
    provide_context=True,
    op_args=[api_url],  # Provide the URL as an argument
    dag=dag,
)

# Task 8: Print Outputs
print_output_task = PythonOperator(
    task_id='print_output_task',
    python_callable=print_output,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
get_starting_count_task >> generate_user_task >> login_task >> get_client_count_task >> create_entity_task >> alias_users_task >> get_client_count_after_alias_task >> login_after_alias_task >> get_client_count_after_alias_login >> print_output_task

if __name__ == "__main__":
    dag.cli()
