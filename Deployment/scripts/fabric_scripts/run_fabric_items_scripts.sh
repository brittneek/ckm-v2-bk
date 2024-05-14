#!/bin/bash
echo "started the script"

# Variables
keyvaultName="$1"
fabricWorkspaceId="$2"
solutionName="$3"

#Replace key vault name and workspace id in the python files
sed -i "s/kv_to-be-replaced/${keyvaultName}/g" "create_fabric_items.py"
sed -i "s/solutionName_to-be-replaced/${solutionName}/g" "create_fabric_items.py"
sed -i "s/workspaceId_to-be-replaced/${fabricWorkspaceId}/g" "create_fabric_items.py"

sed -i "s/kv_to-be-replaced/${keyvaultName}/g" "notebooks/test.ipynb"

pip install -r requirements.txt --quiet

python create_fabric_items.py
