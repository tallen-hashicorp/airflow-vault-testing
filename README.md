## TODO
This is all in one task, make it so login_task is broken out to multiple tasks

## Running airflow
```bash
docker run -p 8080:8080 --name af --rm apache/airflow standalone
docker exec af cat standalone_admin_password.txt
```

## Config Vault
```bash
export VAULT_ADDR='http://127.0.0.1:8200'
vault login

vault policy write test -<<EOF
path "secret/data/test" {
   capabilities = [ "create", "read", "update", "delete" ]
}
EOF

vault auth enable userpass
vault kv put secret/test owner="bob"
```

## Runing DAG
```bash
docker cp vault_entity_dag.py af:/opt/airflow/dags/
docker exec -ti af bash

airflow variables set api_url "http://host.docker.internal:8200"
airflow variables set vault_token "hvs.6EGo6BeynAIJQeTu0VdBb7mh"
airflow variables set num_users 100

airflow dags list
airflow dags trigger vault_entity_dag
```


## Notes
```bash
curl \
    --header "X-Vault-Token: hvs.6EGo6BeynAIJQeTu0VdBb7mh" \
    --request GET \
    http://127.0.0.1:8200/v1/sys/internal/counters/activity/export?start_time=1696973350&end_time=1698701350

vault auth list -output-curl-string

vault write -output-curl-string identity/entity name="bob-smith" policies="base" \
     metadata=organization="ACME Inc." \
     metadata=team="QA" 

curl -X PUT -H "X-Vault-Request: true" -H "X-Vault-Token: $(vault print token)" -d '{"metadata":["organization=ACME Inc.","team=QA"],"name":"bob-smith","policies":"base"}' http://127.0.0.1:8200/v1/identity/entity

vault write -output-curl-string identity/entity-alias name="bob" \
     canonical_id=$(cat entity_id.txt) \
     mount_accessor=$(cat accessor_test.txt) \
     custom_metadata=account="Tester Account"

curl -X PUT -H "X-Vault-Token: $(vault print token)" -H "X-Vault-Request: true" -d '{"canonical_id":"","custom_metadata":"account=Tester Account","mount_accessor":"","name":"bob"}' http://127.0.0.1:8200/v1/identity/entity-alias

vault auth enable -output-curl-string  -path="userpass-test" userpass


curl -X POST -H "X-Vault-Request: true" -H "X-Vault-Token: $(vault print token)" -d '{"type":"userpass","description":"","config":{"options":null,"default_lease_ttl":"0s","max_lease_ttl":"0s","force_no_cache":false},"local":false,"seal_wrap":false,"external_entropy_access":false,"options":null}' http://127.0.0.1:8200/v1/sys/auth/userpass-test
```