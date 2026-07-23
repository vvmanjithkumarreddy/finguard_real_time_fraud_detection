# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Secret Scope
# MAGIC 1. GET API URL and Token
# MAGIC 2. Create a Secret Scope
# MAGIC 3. Add Secrets to the scope

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - GET API URL and Token

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step2 - Create a Secret Scope

# COMMAND ----------

#Define Connection Variables
BOOTSTRAP_SERVERS='pkc-7prvp.centralindia.azure.confluent.cloud:9092'
API_KEY='EGZIJLSBTEYOVM6L'
API_SECRET='cfltGTA9g4kl562oUDPsXSyuSy+AwnEEm+CN+d56S6w2LFxfuADNUSf+bvJswNtg'
TOPIC_NAME='credit_card_transactions'

secret_scope_name = "finguard-scope"
secret_key_name = "kafka_connection_details"

# COMMAND ----------

import requests
import json

#configuration
DATABRICKS_HOST = api_url
DATABRICKS_TOKEN = api_token

url = f"{DATABRICKS_HOST}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}


payload = {
        "scope": secret_scope_name,
        "scope_backend_type": "DATABRICKS"
    }

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret scope '{secret_scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step3 - Add Secrets to the scope

# COMMAND ----------

url = f"{DATABRICKS_HOST}/api/2.0/secrets/put"

connection_config = {
    "BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
    "API_KEY": API_KEY,
    "API_SECRET": API_SECRET,
    "TOPIC_NAME": TOPIC_NAME
}
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": secret_scope_name,
    "key": secret_key_name,
    "string_value": json.dumps(connection_config)
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret '{secret_key_name}' created successfully in scope '{secret_scope_name}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

try:
    secret_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key=secret_key_name
    )
    
    print("Secret retrieved successfully.")
    
    config = json.loads(secret_json)
    print("Parsed JSON:")
    print(config)
    
except Exception as e:
    print("Secret verification failed:")
    print(str(e))



# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding gmail secrets to the scope

# COMMAND ----------

secret_scope_name = "finguard-scope"
secret_key_name = "gmail_api_key"
secret_value = "bcmu bjbv sapd zpne"

# COMMAND ----------

url = f"{DATABRICKS_HOST}/api/2.0/secrets/put"


headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": secret_scope_name,
    "key": secret_key_name,
    "string_value": secret_value
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret '{secret_key_name}' created successfully in scope '{secret_scope_name}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)