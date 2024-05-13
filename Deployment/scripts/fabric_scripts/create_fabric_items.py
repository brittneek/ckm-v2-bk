from azure.identity import DefaultAzureCredential 
import base64
import json
import requests
import pandas as pd
import os

# credential = DefaultAzureCredential()
from azure.identity import AzureCliCredential
credential = AzureCliCredential()

cred = credential.get_token('https://api.fabric.microsoft.com/.default')
token = cred.token

fabric_headers = {"Authorization": "Bearer " + token.strip()}


key_vault_name = 'kv_to-be-replaced'
workspaceId = "workspaceId_to-be-replaced"
solutionname = "solutionName_to-be-replaced"
create_workspace = False

rapper_notebook_name = 'pipeline_notebook'
pipeline_name = 'data_pipeline'
lakehouse_name = 'lakehouse_' + solutionname


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
workspace_name = ws_res.json()['displayName']


#create lakehouse
lakehouse_data = {
  "displayName": lakehouse_name,
  "type": "Lakehouse"
}
lakehouse_res = requests.post(fabric_items_url, headers=fabric_headers, json=lakehouse_data)
# lakehouse_res.json()


# copy local files to lakehouse
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)

account_name = "onelake" #always onelake
data_path = f"{lakehouse_name}.Lakehouse/Files"
folder_path = "data"

account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
service_client = DataLakeServiceClient(account_url, credential=credential)

#Create a file system client for the workspace
file_system_client = service_client.get_file_system_client(workspace_name)

directory_client = file_system_client.get_directory_client(f"{data_path}/{folder_path}")

local_data_path = '../data'
file_names = []
for (dirpath, dirnames, filenames) in os.walk(local_data_path):
    file_names.extend(filenames)
    break

for file_name in file_names:
    file_client = directory_client.get_file_client(file_name)
    with open(file=os.path.join(local_data_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)