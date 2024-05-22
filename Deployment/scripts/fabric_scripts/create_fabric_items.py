from azure.identity import DefaultAzureCredential
import base64
import json
import requests
import pandas as pd
import os
from glob import iglob

from azure.mgmt.resource.subscriptions import SubscriptionClient
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.resource.subscriptions import SubscriptionClient
import uuid
import subprocess

# credential = DefaultAzureCredential()
from azure.identity import AzureCliCredential
credential = AzureCliCredential()

# Initialize the Subscription Client
subscription_client = SubscriptionClient(credential)

print("subscriptions")
subscriptions = list(subscription_client.subscriptions.list())

for sub in subscriptions:
    print(f"Subscription ID: {sub.subscription_id}, Subscription Name: {sub.display_name}")

# For simplicity, we just select the first subscription
selected_subscription = subscriptions[0] if subscriptions else None

if selected_subscription:
    print("Using Subscription ID:", selected_subscription.subscription_id)
else:
    print("No subscriptions available.")

subscription_id = selected_subscription.subscription_id

auth_client = AuthorizationManagementClient(credential, subscription_id)



def get_azure_principal_id():
    """Run the Azure CLI command to get the currently logged-in principal's GUID."""
    # Adjusted command to fetch the object ID of the user
    command = "az account show --query user.name -o json"
    try:
       # Execute the command to get the user's name
        user_name_result = subprocess.check_output(command, shell=True, text=True)
        user_name = json.loads(user_name_result).strip()
        print("username: ", user_name)
        
        # Command to get the user's objectId using the user's name in JSON format
        user_show_cmd = f"az ad user show --id {user_name} --query objectId -o json"
        
        # Execute the command to get the objectId
        object_id_result = subprocess.check_output(user_show_cmd, shell=True, text=True)
        object_id = json.loads(object_id_result).strip()
        
        print('object_id_result: ', object_id_result)
        print('objectid: ', object_id)
        
        return object_id
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        return None

# Usage of the function
principal_id = get_azure_principal_id()
if principal_id:
    print("Principal ID:", principal_id)
else:
    print("Could not retrieve Principal ID.")


# Role details
role_definition_id = '/subscriptions/{}/providers/Microsoft.Authorization/roleDefinitions/{}'.format(subscription_id, '00482a5a-887f-4fb3-b363-3b7fe8e74483')
scope = '/subscriptions/{}'.format(subscription_id)


# Create role assignment
role_assignment_params = RoleAssignmentCreateParameters(
    role_definition_id=role_definition_id,
    principal_id=principal_id
)

role_assignment = auth_client.role_assignments.create(
    scope=scope,
    role_assignment_name=str(uuid.uuid4()),  # Generate a unique UUID for the role assignment
    parameters=role_assignment_params
)

print("Role Assignment created:", role_assignment)



cred = credential.get_token('https://api.fabric.microsoft.com/.default')
token = cred.token

fabric_headers = {"Authorization": "Bearer " + token.strip()}

key_vault_name = 'kv_to-be-replaced'
workspaceId = "workspaceId_to-be-replaced"
solutionname = "solutionName_to-be-replaced"
create_workspace = False

pipeline_notebook_name = 'pipeline_notebook'
pipeline_name = 'data_pipeline'
lakehouse_name = 'lakehouse_' + solutionname

print("workspace id: " ,workspaceId)

if create_workspace == True:
  workspace_name = 'nc_workspace_' + solutionname

  # create workspace
  ws_url = 'https://api.fabric.microsoft.com/v1/workspaces'

  ws_data = {
    "displayName": workspace_name
  }
  ws_res = requests.post(ws_url, headers=fabric_headers, json=ws_data)
  ws_details = ws_res.json()
  # print(ws_details['id'])
  workspaceId = ws_details['id']

fabric_base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/"
fabric_items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/"

fabric_create_workspace_url = f"https://api.fabric.microsoft.com/v1/workspaces"

#get workspace name
ws_res = requests.get(fabric_base_url, headers=fabric_headers)
print("ws_res")
print(ws_res)
workspace_name = ws_res.json()['displayName']

#create lakehouse
lakehouse_data = {
  "displayName": lakehouse_name,
  "type": "Lakehouse"
}
lakehouse_res = requests.post(fabric_items_url, headers=fabric_headers, json=lakehouse_data)

print("lakehouse name: ", lakehouse_name)
print("lakehouse res: ", lakehouse_res)

# copy local files to lakehouse
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)

account_name = "onelake" #always onelake
data_path = f"{lakehouse_name}.Lakehouse/Files"
folder_path = "/"

account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
service_client = DataLakeServiceClient(account_url, credential=credential)

#Create a file system client for the workspace
file_system_client = service_client.get_file_system_client(workspace_name)

directory_client = file_system_client.get_directory_client(f"{data_path}/{folder_path}")

local_path = 'data/**/*'
file_names = [f for f in iglob(local_path, recursive=True) if os.path.isfile(f)]
for file_name in file_names:
  file_client = directory_client.get_file_client(file_name)
  with open(file=file_name, mode="rb") as data:
    file_client.upload_data(data, overwrite=True)

#get environments
try:
  fabric_env_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/environments/"
  env_res = requests.get(fabric_env_url, headers=fabric_headers)
  env_res_id = env_res.json()['value'][0]['id']
  # print(env_res.json())
except:
  env_res_id = ''

#create notebook items
notebook_names =['00_process_json_files','01_process_audio_files', '02_enrich_data','03_create_calendar_data', '04_generate_and_multiple_data', 'pipeline_notebook']
# notebook_names =['process_data_new']

for notebook_name in notebook_names:

    with open('notebooks/'+ notebook_name +'.ipynb', 'r') as f:
        notebook_json = json.load(f)

    # notebook_json['metadata']['trident']['lakehouse']['default_lakehouse'] = lakehouse_res.json()['id']
    # notebook_json['metadata']['trident']['lakehouse']['default_lakehouse_name'] = lakehouse_res.json()['displayName']
    # notebook_json['metadata']['trident']['lakehouse']['workspaceId'] = lakehouse_res.json()['workspaceId']

    print("lakehouse_res")
    print(lakehouse_res)
    print(lakehouse_res.json())

    notebook_json['metadata']['dependencies']['lakehouse']['default_lakehouse'] = lakehouse_res.json()['id']
    notebook_json['metadata']['dependencies']['lakehouse']['default_lakehouse_name'] = lakehouse_res.json()['displayName']
    notebook_json['metadata']['dependencies']['lakehouse']['workspaceId'] = lakehouse_res.json()['workspaceId']

    if env_res_id != '':
        try:
            notebook_json['metadata']['dependencies']['environment']['environmentId'] = env_res_id
            notebook_json['metadata']['dependencies']['environment']['workspaceId'] = lakehouse_res.json()['workspaceId']
        except:
            pass


    notebook_base64 = base64.b64encode(json.dumps(notebook_json).encode('utf-8'))

    notebook_data = {
        "displayName":notebook_name,
        "type":"Notebook",
        "definition" : {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": notebook_base64.decode('utf-8'),
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    fabric_response = requests.post(fabric_items_url, headers=fabric_headers, json=notebook_data)
    #print(fabric_response.json())

# get wrapper notebook id
fabric_notebooks_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks"
notebooks_res = requests.get(fabric_notebooks_url, headers=fabric_headers)
notebooks_res.json()

pipeline_notebook_id = ''
for n in notebooks_res.json().values():
    for notebook in n:
        if notebook['displayName'] == pipeline_notebook_name:
            pipeline_notebook_id = notebook['id']
            break
print(pipeline_notebook_id)


# create pipeline item
pipeline_json = {
    "name": pipeline_name,
    "properties": {
        "activities": [
            {
                "name": "process_data",
                "type": "TridentNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": "false",
                    "secureInput": "false"
                },
                "typeProperties": {
                    "notebookId": pipeline_notebook_id,
                    "workspaceId": workspaceId,
                    # "parameters": {
                    #     "input_scenario": {
                    #         "value": {
                    #             "value": "@pipeline().parameters.input_scenario",
                    #             "type": "Expression"
                    #         },
                    #         "type": "string"
                    #     }
                    # }
                }
            }
        ],
        # "parameters": {
        #     "input_scenario": {
        #         "type": "string",
        #         "defaultValue": "fsi"
        #     }
        # }
    }
}

pipeline_base64 = base64.b64encode(json.dumps(pipeline_json).encode('utf-8'))

pipeline_data = {
        "displayName":pipeline_name,
        "type":"DataPipeline",
        "definition" : {
            # "format": "json",
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": pipeline_base64.decode('utf-8'),
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

pipeline_response = requests.post(fabric_items_url, headers=fabric_headers, json=pipeline_data)
pipeline_response.json()

# run the pipeline once
job_url = fabric_base_url + f"items/{pipeline_response.json()['id']}/jobs/instances?jobType=Pipeline"
job_response = requests.post(job_url, headers=fabric_headers)