// ========== main.bicep ========== //
targetScope = 'resourceGroup'

@minLength(3)
@maxLength(6)
@description('Prefix Name')
param solutionPrefix string

// @description('Fabric Workspace Id if you have one, else leave it empty. ')
// param fabricWorkspaceId string

var resourceGroupLocation = resourceGroup().location
var resourceGroupName = resourceGroup().name
var subscriptionId  = subscription().subscriptionId


var solutionLocation = resourceGroupLocation
var baseUrl = 'https://raw.githubusercontent.com/microsoft/Customer-Service-Conversational-Insights-with-Azure-OpenAI-Services/master/'


// ========== Managed Identity ========== //
module managedIdentityModule 'deploy_managed_identity.bicep' = {
  name: 'deploy_managed_identity'
  params: {
    solutionName: solutionPrefix
    solutionLocation: solutionLocation
  }
  scope: resourceGroup(resourceGroup().name)
}

// ========== Azure OpenAI ========== //
module azOpenAI 'deploy_azure_open_ai.bicep' = {
  name: 'deploy_azure_open_ai'
  params: {
    solutionName: solutionPrefix
    solutionLocation: solutionLocation
  }
}

// ========== Key Vault ========== //

module keyvaultModule 'deploy_keyvault.bicep' = {
  name: 'deploy_keyvault'
  params: {
    solutionName: solutionPrefix
    solutionLocation: solutionLocation
    objectId: managedIdentityModule.outputs.managedIdentityOutput.objectId
    tenantId: subscription().tenantId
    managedIdentityObjectId:managedIdentityModule.outputs.managedIdentityOutput.objectId
    adlsAccountName:storageAccountModule.outputs.storageAccountOutput.storageAccountName
    adlsAccountKey:storageAccountModule.outputs.storageAccountOutput.key
    azureOpenAIApiKey:azOpenAI.outputs.openAIOutput.openAPIKey
    azureOpenAIApiVersion:'2023-07-01-preview'
    azureOpenAIEndpoint:azOpenAI.outputs.openAIOutput.openAPIEndpoint
    azureSearchAdminKey:azSearchService.outputs.searchServiceOutput.searchServiceAdminKey
    azureSearchServiceEndpoint:azSearchService.outputs.searchServiceOutput.searchServiceEndpoint
    azureSearchServiceName:azSearchService.outputs.searchServiceOutput.searchServiceName
    azureSearchArticlesIndex:'articlesindex'
    azureSearchGrantsIndex:'grantsindex'
    azureSearchDraftsIndex:'draftsindex'
    cogServiceEndpoint:azAIMultiServiceAccount.outputs.cogSearchOutput.cogServiceEndpoint
    cogServiceName:azAIMultiServiceAccount.outputs.cogSearchOutput.cogServiceName
    cogServiceKey:azAIMultiServiceAccount.outputs.cogSearchOutput.cogServiceKey
    enableSoftDelete:false
  }
  scope: resourceGroup(resourceGroup().name)
  dependsOn:[storageAccountModule,azOpenAI,azAIMultiServiceAccount,azSearchService]
}

// ========== Fabric ========== //
module createFabricItems 'deploy_fabric_scripts.bicep' = if (fabricWorkspaceId != '') {
  name : 'deploy_fabric_scripts'
  params:{
    solutionLocation: solutionLocation
    identity:managedIdentityModule.outputs.managedIdentityOutput.id
    baseUrl:baseUrl
    keyVaultName:keyvaultModule.outputs.keyvaultOutput.name
    fabricWorkspaceId:fabricWorkspaceId
  }
  dependsOn:[keyvaultModule]
}
