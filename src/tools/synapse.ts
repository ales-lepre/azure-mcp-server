import { SynapseManagementClient } from '@azure/arm-synapse';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class SynapseTools {
  private managementClient: SynapseManagementClient;
  private synapseToken?: string;
  private workspaceUrl?: string;

  constructor(private config: AzureConfig & { synapseToken?: string; synapseWorkspaceUrl?: string }) {
    if (!config.subscriptionId) {
      throw new Error('Subscription ID is required for Synapse operations');
    }

    const credential = getAzureCredential(config);
    this.managementClient = new SynapseManagementClient(credential, config.subscriptionId);
    this.synapseToken = config.synapseToken;
    this.workspaceUrl = config.synapseWorkspaceUrl;
  }

  async listWorkspaces(resourceGroupName: string) {
    try {
      const workspaces = [];
      for await (const workspace of this.managementClient.workspaces.listByResourceGroup(resourceGroupName)) {
        workspaces.push({
          name: workspace.name,
          location: workspace.location,
          provisioningState: workspace.provisioningState,
          defaultDataLakeStorage: workspace.defaultDataLakeStorage,
          managedResourceGroupName: workspace.managedResourceGroupName,
          sqlAdministratorLogin: workspace.sqlAdministratorLogin,
          identity: workspace.identity,
          virtualNetworkProfile: workspace.virtualNetworkProfile,
          connectivityEndpoints: workspace.connectivityEndpoints,
          managedVirtualNetwork: workspace.managedVirtualNetwork,
          privateEndpointConnections: workspace.privateEndpointConnections,
          workspaceUID: workspace.workspaceUID,
          extraProperties: workspace.extraProperties,
          managedVirtualNetworkSettings: workspace.managedVirtualNetworkSettings,
          encryption: workspace.encryption,
          workspaceRepositoryConfiguration: workspace.workspaceRepositoryConfiguration,
          publicNetworkAccess: workspace.publicNetworkAccess,
          cspWorkspaceAdminProperties: workspace.cspWorkspaceAdminProperties,
          settings: workspace.settings,
          azureADOnlyAuthentication: workspace.azureADOnlyAuthentication,
          trustedServiceBypassEnabled: workspace.trustedServiceBypassEnabled
        });
      }
      return { workspaces };
    } catch (error) {
      throw new Error(`Failed to list Synapse workspaces: ${error}`);
    }
  }

  async getWorkspace(resourceGroupName: string, workspaceName: string) {
    try {
      const workspace = await this.managementClient.workspaces.get(resourceGroupName, workspaceName);
      return {
        name: workspace.name,
        location: workspace.location,
        provisioningState: workspace.provisioningState,
        defaultDataLakeStorage: workspace.defaultDataLakeStorage,
        managedResourceGroupName: workspace.managedResourceGroupName,
        sqlAdministratorLogin: workspace.sqlAdministratorLogin,
        identity: workspace.identity,
        virtualNetworkProfile: workspace.virtualNetworkProfile,
        connectivityEndpoints: workspace.connectivityEndpoints,
        managedVirtualNetwork: workspace.managedVirtualNetwork,
        privateEndpointConnections: workspace.privateEndpointConnections,
        workspaceUID: workspace.workspaceUID,
        extraProperties: workspace.extraProperties,
        managedVirtualNetworkSettings: workspace.managedVirtualNetworkSettings,
        encryption: workspace.encryption,
        workspaceRepositoryConfiguration: workspace.workspaceRepositoryConfiguration,
        publicNetworkAccess: workspace.publicNetworkAccess,
        cspWorkspaceAdminProperties: workspace.cspWorkspaceAdminProperties,
        settings: workspace.settings,
        azureADOnlyAuthentication: workspace.azureADOnlyAuthentication,
        trustedServiceBypassEnabled: workspace.trustedServiceBypassEnabled,
        tags: workspace.tags
      };
    } catch (error) {
      throw new Error(`Failed to get Synapse workspace: ${error}`);
    }
  }

  async listSqlPools(resourceGroupName: string, workspaceName: string) {
    try {
      const sqlPools = [];
      for await (const pool of this.managementClient.sqlPools.listByWorkspace(resourceGroupName, workspaceName)) {
        sqlPools.push({
          name: pool.name,
          location: pool.location,
          sku: pool.sku,
          status: pool.status,
          maxSizeBytes: pool.maxSizeBytes,
          collation: pool.collation,
          sourceDatabaseId: pool.sourceDatabaseId,
          restorePointInTime: pool.restorePointInTime,
          createMode: pool.createMode,
          creationDate: pool.creationDate,
          storageAccountType: pool.storageAccountType,
          provisioningState: pool.provisioningState,
          tags: pool.tags
        });
      }
      return { sqlPools };
    } catch (error) {
      throw new Error(`Failed to list SQL pools: ${error}`);
    }
  }

  async getSqlPool(resourceGroupName: string, workspaceName: string, sqlPoolName: string) {
    try {
      const pool = await this.managementClient.sqlPools.get(resourceGroupName, workspaceName, sqlPoolName);
      return {
        name: pool.name,
        location: pool.location,
        sku: pool.sku,
        status: pool.status,
        maxSizeBytes: pool.maxSizeBytes,
        collation: pool.collation,
        sourceDatabaseId: pool.sourceDatabaseId,
        restorePointInTime: pool.restorePointInTime,
        createMode: pool.createMode,
        creationDate: pool.creationDate,
        storageAccountType: pool.storageAccountType,
        provisioningState: pool.provisioningState,
        tags: pool.tags
      };
    } catch (error) {
      throw new Error(`Failed to get SQL pool: ${error}`);
    }
  }

  async pauseSqlPool(resourceGroupName: string, workspaceName: string, sqlPoolName: string) {
    try {
      await this.managementClient.sqlPools.beginPause(resourceGroupName, workspaceName, sqlPoolName);
      return { message: `SQL pool ${sqlPoolName} pause initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to pause SQL pool: ${error}`);
    }
  }

  async resumeSqlPool(resourceGroupName: string, workspaceName: string, sqlPoolName: string) {
    try {
      await this.managementClient.sqlPools.beginResume(resourceGroupName, workspaceName, sqlPoolName);
      return { message: `SQL pool ${sqlPoolName} resume initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to resume SQL pool: ${error}`);
    }
  }

  async listSparkPools(resourceGroupName: string, workspaceName: string) {
    try {
      const sparkPools = [];
      for await (const pool of this.managementClient.bigDataPools.listByWorkspace(resourceGroupName, workspaceName)) {
        sparkPools.push({
          name: pool.name,
          location: pool.location,
          provisioningState: pool.provisioningState,
          autoScale: pool.autoScale,
          creationDate: pool.creationDate,
          autoPause: pool.autoPause,
          isComputeIsolationEnabled: pool.isComputeIsolationEnabled,
          sessionLevelPackagesEnabled: pool.sessionLevelPackagesEnabled,
          cacheSize: pool.cacheSize,
          dynamicExecutorAllocation: pool.dynamicExecutorAllocation,
          sparkEventsFolder: pool.sparkEventsFolder,
          nodeCount: pool.nodeCount,
          libraryRequirements: pool.libraryRequirements,
          customLibraries: pool.customLibraries,
          sparkConfigProperties: pool.sparkConfigProperties,
          sparkVersion: pool.sparkVersion,
          defaultSparkLogFolder: pool.defaultSparkLogFolder,
          nodeSize: pool.nodeSize,
          nodeSizeFamily: pool.nodeSizeFamily,
          lastSucceededTimestamp: pool.lastSucceededTimestamp,
          tags: pool.tags
        });
      }
      return { sparkPools };
    } catch (error) {
      throw new Error(`Failed to list Spark pools: ${error}`);
    }
  }

  async getSparkPool(resourceGroupName: string, workspaceName: string, sparkPoolName: string) {
    try {
      const pool = await this.managementClient.bigDataPools.get(resourceGroupName, workspaceName, sparkPoolName);
      return {
        name: pool.name,
        location: pool.location,
        provisioningState: pool.provisioningState,
        autoScale: pool.autoScale,
        creationDate: pool.creationDate,
        autoPause: pool.autoPause,
        isComputeIsolationEnabled: pool.isComputeIsolationEnabled,
        sessionLevelPackagesEnabled: pool.sessionLevelPackagesEnabled,
        cacheSize: pool.cacheSize,
        dynamicExecutorAllocation: pool.dynamicExecutorAllocation,
        sparkEventsFolder: pool.sparkEventsFolder,
        nodeCount: pool.nodeCount,
        libraryRequirements: pool.libraryRequirements,
        customLibraries: pool.customLibraries,
        sparkConfigProperties: pool.sparkConfigProperties,
        sparkVersion: pool.sparkVersion,
        defaultSparkLogFolder: pool.defaultSparkLogFolder,
        nodeSize: pool.nodeSize,
        nodeSizeFamily: pool.nodeSizeFamily,
        lastSucceededTimestamp: pool.lastSucceededTimestamp,
        tags: pool.tags
      };
    } catch (error) {
      throw new Error(`Failed to get Spark pool: ${error}`);
    }
  }

  async listIntegrationRuntimes(resourceGroupName: string, workspaceName: string) {
    try {
      const integrationRuntimes = [];
      for await (const runtime of this.managementClient.integrationRuntimes.listByWorkspace(resourceGroupName, workspaceName)) {
        integrationRuntimes.push({
          name: runtime.name,
          type: runtime.type,
          properties: runtime.properties,
          etag: runtime.etag
        });
      }
      return { integrationRuntimes };
    } catch (error) {
      throw new Error(`Failed to list integration runtimes: ${error}`);
    }
  }

  async getIntegrationRuntime(resourceGroupName: string, workspaceName: string, integrationRuntimeName: string) {
    try {
      const runtime = await this.managementClient.integrationRuntimes.get(resourceGroupName, workspaceName, integrationRuntimeName);
      return {
        name: runtime.name,
        type: runtime.type,
        properties: runtime.properties,
        etag: runtime.etag
      };
    } catch (error) {
      throw new Error(`Failed to get integration runtime: ${error}`);
    }
  }

  private async makeApiRequest(endpoint: string, method: string = 'GET', body?: any) {
    if (!this.synapseToken || !this.workspaceUrl) {
      throw new Error('Synapse token and workspace URL are required for API operations');
    }

    const url = `${this.workspaceUrl}/${endpoint}`;
    const options: RequestInit = {
      method,
      headers: {
        'Authorization': `Bearer ${this.synapseToken}`,
        'Content-Type': 'application/json'
      }
    };

    if (body && method !== 'GET') {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Synapse API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
  }

  async listPipelines() {
    try {
      const response = await this.makeApiRequest('pipelines?api-version=2020-12-01');
      return {
        pipelines: response.value?.map((pipeline: any) => ({
          name: pipeline.name,
          type: pipeline.type,
          etag: pipeline.etag,
          properties: {
            description: pipeline.properties?.description,
            activities: pipeline.properties?.activities,
            parameters: pipeline.properties?.parameters,
            variables: pipeline.properties?.variables,
            concurrency: pipeline.properties?.concurrency,
            annotations: pipeline.properties?.annotations,
            runDimensions: pipeline.properties?.runDimensions,
            folder: pipeline.properties?.folder
          }
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list pipelines: ${error}`);
    }
  }

  async getPipeline(pipelineName: string) {
    try {
      const response = await this.makeApiRequest(`pipelines/${pipelineName}?api-version=2020-12-01`);
      return {
        name: response.name,
        type: response.type,
        etag: response.etag,
        properties: response.properties
      };
    } catch (error) {
      throw new Error(`Failed to get pipeline: ${error}`);
    }
  }

  async createPipelineRun(pipelineName: string, parameters?: Record<string, any>) {
    try {
      const body = parameters ? { parameters } : {};
      const response = await this.makeApiRequest(`pipelines/${pipelineName}/createRun?api-version=2020-12-01`, 'POST', body);
      return {
        runId: response.runId,
        message: `Pipeline run started successfully`
      };
    } catch (error) {
      throw new Error(`Failed to create pipeline run: ${error}`);
    }
  }

  async getPipelineRun(runId: string) {
    try {
      const response = await this.makeApiRequest(`pipelineruns/${runId}?api-version=2020-12-01`);
      return {
        runId: response.runId,
        pipelineName: response.pipelineName,
        status: response.status,
        runStart: response.runStart,
        runEnd: response.runEnd,
        durationInMs: response.durationInMs,
        parameters: response.parameters,
        message: response.message,
        lastUpdated: response.lastUpdated,
        invokedBy: response.invokedBy,
        isLatest: response.isLatest
      };
    } catch (error) {
      throw new Error(`Failed to get pipeline run: ${error}`);
    }
  }

  async cancelPipelineRun(runId: string) {
    try {
      await this.makeApiRequest(`pipelineruns/${runId}/cancel?api-version=2020-12-01`, 'POST');
      return { message: `Pipeline run ${runId} cancellation initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to cancel pipeline run: ${error}`);
    }
  }

  async listNotebooks() {
    try {
      const response = await this.makeApiRequest('notebooks?api-version=2020-12-01');
      return {
        notebooks: response.value?.map((notebook: any) => ({
          name: notebook.name,
          type: notebook.type,
          etag: notebook.etag,
          properties: {
            description: notebook.properties?.description,
            bigDataPool: notebook.properties?.bigDataPool,
            sessionProperties: notebook.properties?.sessionProperties,
            metadata: notebook.properties?.metadata,
            nbformat: notebook.properties?.nbformat,
            nbformatMinor: notebook.properties?.nbformat_minor,
            folder: notebook.properties?.folder
          }
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list notebooks: ${error}`);
    }
  }

  async getNotebook(notebookName: string) {
    try {
      const response = await this.makeApiRequest(`notebooks/${notebookName}?api-version=2020-12-01`);
      return {
        name: response.name,
        type: response.type,
        etag: response.etag,
        properties: response.properties
      };
    } catch (error) {
      throw new Error(`Failed to get notebook: ${error}`);
    }
  }

  async listDataflows() {
    try {
      const response = await this.makeApiRequest('dataflows?api-version=2020-12-01');
      return {
        dataflows: response.value?.map((dataflow: any) => ({
          name: dataflow.name,
          type: dataflow.type,
          etag: dataflow.etag,
          properties: {
            description: dataflow.properties?.description,
            annotations: dataflow.properties?.annotations,
            folder: dataflow.properties?.folder
          }
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list dataflows: ${error}`);
    }
  }

  async getDataflow(dataflowName: string) {
    try {
      const response = await this.makeApiRequest(`dataflows/${dataflowName}?api-version=2020-12-01`);
      return {
        name: response.name,
        type: response.type,
        etag: response.etag,
        properties: response.properties
      };
    } catch (error) {
      throw new Error(`Failed to get dataflow: ${error}`);
    }
  }

  async listDatasets() {
    try {
      const response = await this.makeApiRequest('datasets?api-version=2020-12-01');
      return {
        datasets: response.value?.map((dataset: any) => ({
          name: dataset.name,
          type: dataset.type,
          etag: dataset.etag,
          properties: {
            type: dataset.properties?.type,
            description: dataset.properties?.description,
            structure: dataset.properties?.structure,
            schema: dataset.properties?.schema,
            linkedServiceName: dataset.properties?.linkedServiceName,
            parameters: dataset.properties?.parameters,
            annotations: dataset.properties?.annotations,
            folder: dataset.properties?.folder
          }
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list datasets: ${error}`);
    }
  }

  async getDataset(datasetName: string) {
    try {
      const response = await this.makeApiRequest(`datasets/${datasetName}?api-version=2020-12-01`);
      return {
        name: response.name,
        type: response.type,
        etag: response.etag,
        properties: response.properties
      };
    } catch (error) {
      throw new Error(`Failed to get dataset: ${error}`);
    }
  }

  async listLinkedServices() {
    try {
      const response = await this.makeApiRequest('linkedservices?api-version=2020-12-01');
      return {
        linkedServices: response.value?.map((service: any) => ({
          name: service.name,
          type: service.type,
          etag: service.etag,
          properties: {
            type: service.properties?.type,
            description: service.properties?.description,
            connectVia: service.properties?.connectVia,
            parameters: service.properties?.parameters,
            annotations: service.properties?.annotations
          }
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list linked services: ${error}`);
    }
  }

  async getLinkedService(linkedServiceName: string) {
    try {
      const response = await this.makeApiRequest(`linkedservices/${linkedServiceName}?api-version=2020-12-01`);
      return {
        name: response.name,
        type: response.type,
        etag: response.etag,
        properties: response.properties
      };
    } catch (error) {
      throw new Error(`Failed to get linked service: ${error}`);
    }
  }
}