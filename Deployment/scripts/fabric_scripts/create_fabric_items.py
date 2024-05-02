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

key_vault_name = 'kv_to-be-replaced'
workspaceId = "workspaceId_to-be-replaced"

fabric_headers = {"Authorization": "Bearer " + token.strip()}
fabric_base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/"
fabric_items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/"


print("items url")
print(fabric_items_url)

lakehouse_name = 'Lakehouse2'

lakehouse_data = {
  "displayName": lakehouse_name,
  "type": "Lakehouse"
}
print("lakehouse data")
print(lakehouse_data)
lakehouse_res = requests.post(fabric_items_url, headers=fabric_headers, json=lakehouse_data)

print(lakehouse_res)


# notebook_names =['test']

# for notebook_name in notebook_names:

#     with open('notebooks/'+ notebook_name +'.ipynb', 'r') as f:
#         notebook_json = json.load(f)

#     notebook_json['metadata']['trident']['lakehouse']['default_lakehouse'] = lakehouse_res.json()['id']
#     notebook_json['metadata']['trident']['lakehouse']['default_lakehouse_name'] = lakehouse_res.json()['displayName']
#     notebook_json['metadata']['trident']['lakehouse']['workspaceId'] = lakehouse_res.json()['workspaceId']

#     notebook_base64 = base64.b64encode(json.dumps(notebook_json).encode('utf-8'))

#     notebook_data = {
#         "displayName":notebook_name,
#         "type":"Notebook",
#         "definition" : {
#             "format": "ipynb",
#             "parts": [
#                 {
#                     "path": "notebook-content.ipynb",
#                     "payload": notebook_base64.decode('utf-8'),
#                     "payloadType": "InlineBase64"
#                 }
#             ]
#         }
#     }
#     fabric_response = requests.post(fabric_items_url, headers=fabric_headers, json=notebook_data)
    #print(fabric_response.json())    