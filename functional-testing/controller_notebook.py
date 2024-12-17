# Databricks notebook source
# dbutils.widgets.text("functional_test", "retail")
dbutils.widgets.dropdown("functional_test", "farmers", ["farmers", "retail"])

# COMMAND ----------

import requests
import json

def start_job_run(job_id, parameters):
    url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    data = {
        "job_id": job_id,
        "job_parameters": parameters
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("Job run started successfully")
        return response.json()
    else:
        print(f"Failed to start job run: {response.text}")
        return None

selected_value = dbutils.widgets.get("functional_test")
if selected_value == "farmers":
    job_id = "979897659230918"
    parameters = {"functional_test": "farmers"}
    status = start_job_run(job_id, parameters)
elif selected_value == "retail":
    job_id = "460851111844889"
    parameters = {"functional_test": "retail"}
    status = start_job_run(job_id, parameters)

# COMMAND ----------

# import requests
# import json



# def start_dlt_pipeline(pipeline_id):
#     url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/pipelines/{pipeline_id}/updates"
#     headers = {
#         "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
#         "Content-Type": "application/json"
#     }
#     response = requests.post(url, headers=headers)
#     if response.status_code == 200:
#         print("Pipeline started successfully")
#     else:
#         print(f"Failed to start pipeline: {response.text}")

# if dbutils.widgets.get("functional-test") == "okta":
#     pipeline_id = "1e3765b1-79a4-47e7-82b9-885c037d2fde"
#     status = start_dlt_pipeline(pipeline_id)
