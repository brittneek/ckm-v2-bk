#!/bin/bash
echo "started the script"

# Variables
keyvaultName="$2"
fabricWorkspaceId="$3"
solutionName="$4"

# echo "Download Started"

# # Download the create_fabric python files
# curl --output "create_fabric_items.py" ${baseUrl}"Deployment/scripts/fabric_scripts/create_fabric_items.py"

# curl --output "create_articles_index.ipynb" ${baseUrl}"Deployment/scripts/fabric_scripts/create_articles_index.ipynb"

# # Download the requirement file
# curl --output "$requirementFile" "$requirementFileUrl"

# echo "Download completed"

#Replace key vault name and workspace id in the python files
sed -i "s/kv_to-be-replaced/${keyvaultName}/g" "create_fabric_items.py"
sed -i "s/solutionName_to-be-replaced/${solutionName}/g" "create_fabric_items.py"
sed -i "s/workspaceId_to-be-replaced/${fabricWorkspaceId}/g" "create_fabric_items.py"

sed -i "s/kv_to-be-replaced/${keyvaultName}/g" "create_articles_index.ipynb"

pip install -r requirements.txt

python create_fabric_items.py
