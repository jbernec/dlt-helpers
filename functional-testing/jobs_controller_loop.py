# Databricks notebook source
dbutils.widgets.dropdown("functional_test", "farmers", ["farmers", "retail", "okta"])

# COMMAND ----------

import requests
import json

def create_job(job_name):
    url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/jobs/create"
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    data = {
        "name": job_name,
        "email_notifications": {
            "on_success": ["charles@email.com"],
            "on_failure": ["charles@email.com"]
        },
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 2
        },
        "tasks": [
            {
                "task_key": "initial_task",
                "notebook_task": {
                    "notebook_path": "/Path/To/Your/Notebook"
                },
                "new_cluster": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2
                }
            },
            {
                "task_key": "dependent_task",
                "depends_on": [
                    {
                        "task_key": "initial_task"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Path/To/Your/DependentNotebook"
                },
                "new_cluster": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2
                }
            }
        ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Job {job_name} created successfully with job_id: {job_id}")
        return job_id
    else:
        print(f"Failed to create job {job_name}: {response.text}")
        return None

def start_job_run(job_id, parameters):
    url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    data = {
        "job_id": job_id,
        "notebook_params": parameters
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print(f"Job run for job_id {job_id} started successfully")
        return response.json()
    else:
        print(f"Failed to start job run for job_id {job_id}: {response.text}")
        return None

functional_tests = ["farmers", "retail", "okta"]
job_ids = {}

for test in functional_tests:
    job_id = create_job(test)
    if job_id:
        job_ids[test] = job_id

for test, job_id in job_ids.items():
    parameters = {"functional_test": test}
    start_job_run(job_id, parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DLT Update

# COMMAND ----------

import requests
import json

def create_dlt_pipeline(pipeline_name, notebook_path):
    url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/pipelines"
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    data = {
        "name": pipeline_name,
        "storage": "/path/to/storage",
        "configuration": {
            "notebook_path": notebook_path
        },
        "clusters": [
            {
                "label": "default",
                "num_workers": 2
            }
        ],
        "libraries": [],
        "target": "default",
        "continuous": False,
        "development": True
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        pipeline_id = response.json().get("pipeline_id")
        print(f"DLT pipeline {pipeline_name} created successfully with pipeline_id: {pipeline_id}")
        return pipeline_id
    else:
        print(f"Failed to create DLT pipeline {pipeline_name}: {response.text}")
        return None

def create_job(job_name, pipeline_id):
    url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/jobs/create"
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    data = {
        "name": job_name,
        "email_notifications": {
            "on_success": ["charles@email.com"],
            "on_failure": ["charles@email.com"]
        },
        "tasks": [
            {
                "task_key": "initial_task",
                "notebook_task": {
                    "notebook_path": "/Path/To/Your/Notebook"
                },
                "new_cluster": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2
                }
            },
            {
                "task_key": "dlt_task",
                "depends_on": [
                    {
                        "task_key": "initial_task"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
                "new_cluster": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2
                }
            }
        ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Job {job_name} created successfully with job_id: {job_id}")
        return job_id
    else:
        print(f"Failed to create job {job_name}: {response.text}")
        return None

def start_job_run(job_id, parameters):
    url = f"https://adb-362282074994262.2.azuredatabricks.net/api/2.0/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('myscope', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    data = {
        "job_id": job_id,
        "notebook_params": parameters
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print(f"Job run for job_id {job_id} started successfully")
        return response.json()
    else:
        print(f"Failed to start job run for job_id {job_id}: {response.text}")
        return None

functional_tests = ["farmers", "retail", "okta"]
job_ids = {}

for test in functional_tests:
    pipeline_id = create_dlt_pipeline(f"{test}_pipeline", f"/Path/To/{test}/Notebook")
    if pipeline_id:
        job_id = create_job(test, pipeline_id)
        if job_id:
            job_ids[test] = job_id

for test, job_id in job_ids.items():
    parameters = {"functional_test": test}
    start_job_run(job_id, parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sample Job JSON

# COMMAND ----------

{
  "name": "job_farmers_functional_test",
  "email_notifications": {
    "on_success": [
      "chchukwu@microsoft.com"
    ],
    "on_failure": [
      "chchukwu@microsoft.com"
    ],
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false
  },
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "process_data",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/admin@mngenvmcap374485.onmicrosoft.com/dlt-helpers/functional-testing/farmers/generate_data",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "Job_cluster",
      "timeout_seconds": 0,
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "dlt_farmers",
      "depends_on": [
        {
          "task_key": "process_data"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "pipeline_task": {
        "pipeline_id": "1e3765b1-79a4-47e7-82b9-885c037d2fde",
        "full_refresh": false
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "query_expectations",
      "depends_on": [
        {
          "task_key": "dlt_farmers"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/admin@mngenvmcap374485.onmicrosoft.com/dlt-helpers/functional-testing/farmers/query_expectations_events",
        "base_parameters": {
          "test_param": "test_val"
        },
        "source": "WORKSPACE"
      },
      "job_cluster_key": "Job_cluster",
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      },
      "webhook_notifications": {}
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "Job_cluster",
      "new_cluster": {
        "cluster_name": "",
        "spark_version": "15.4.x-scala2.12",
        "azure_attributes": {
          "first_on_demand": 1,
          "availability": "ON_DEMAND_AZURE",
          "spot_bid_max_price": -1
        },
        "node_type_id": "Standard_D4ds_v5",
        "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": true,
        "data_security_mode": "USER_ISOLATION",
        "runtime_engine": "PHOTON",
        "num_workers": 8
      }
    }
  ],
  "queue": {
    "enabled": true
  },
  "parameters": [
    {
      "name": "functional_test",
      "default": "farmers"
    }
  ],
  "run_as": {
    "user_name": "admin@mngenvmcap374485.onmicrosoft.com"
  }
}
