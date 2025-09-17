#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';

import { getConfig } from './config.js';
import { DataLakeTools } from './tools/datalake.js';
import { StorageTools } from './tools/storage.js';
import { FabricTools } from './tools/fabric.js';
import { DataFactoryTools } from './tools/datafactory.js';
import { SqlDatabaseTools } from './tools/sqldatabase.js';
import { DatabricksTools } from './tools/databricks.js';
import { MonitorTools } from './tools/monitor.js';
import { SynapseTools } from './tools/synapse.js';
import { DimensionalModelingTools } from './tools/dimensional.js';
import { FabricPipelineOrchestrator } from './tools/fabricpipelines.js';
import { DataWarehouseManagement } from './tools/datawarehouse.js';

const server = new Server({
  name: 'azure-mcp-server',
  version: '1.0.0',
});

// Initialize tools
const config = getConfig();
let dataLakeTools: DataLakeTools | null = null;
let storageTools: StorageTools | null = null;
let fabricTools: FabricTools | null = null;
let dataFactoryTools: DataFactoryTools | null = null;
let sqlDatabaseTools: SqlDatabaseTools | null = null;
let databricksTools: DatabricksTools | null = null;
let monitorTools: MonitorTools | null = null;
let synapseTools: SynapseTools | null = null;
let dimensionalTools: DimensionalModelingTools | null = null;
let fabricPipelineTools: FabricPipelineOrchestrator | null = null;
let dataWarehouseTools: DataWarehouseManagement | null = null;

try {
  if (config.dataLakeAccountName) {
    dataLakeTools = new DataLakeTools(config);
  }
  if (config.storageAccountName) {
    storageTools = new StorageTools(config);
  }
  if (config.subscriptionId) {
    fabricTools = new FabricTools(config);
    dataFactoryTools = new DataFactoryTools(config);
    sqlDatabaseTools = new SqlDatabaseTools(config);
    monitorTools = new MonitorTools(config);
  }
  if (config.databricksToken && config.databricksWorkspaceUrl) {
    databricksTools = new DatabricksTools(config);
  }
  if (config.synapseToken && config.synapseWorkspaceUrl) {
    synapseTools = new SynapseTools(config);
  }

  // Always available tools (don't require specific service credentials)
  dimensionalTools = new DimensionalModelingTools(config);
  dataWarehouseTools = new DataWarehouseManagement(config);

  // Fabric pipeline tools (if Fabric token available)
  if (config.fabricToken && config.fabricWorkspaceUrl) {
    fabricPipelineTools = new FabricPipelineOrchestrator(config);
  }
} catch (error) {
  console.error('Error initializing tools:', error);
}

// Data Lake Storage tools
server.setRequestHandler(ListToolsRequestSchema, async () => {
  const tools = [];

  // Data Lake tools
  if (dataLakeTools) {
    tools.push(
      {
        name: 'datalake_list_filesystems',
        description: 'List all file systems in the Data Lake storage account',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'datalake_create_filesystem',
        description: 'Create a new file system in the Data Lake',
        inputSchema: {
          type: 'object',
          properties: {
            name: { type: 'string', description: 'Name of the file system to create' },
          },
          required: ['name'],
        },
      },
      {
        name: 'datalake_list_files',
        description: 'List files and directories in a Data Lake file system',
        inputSchema: {
          type: 'object',
          properties: {
            fileSystemName: { type: 'string', description: 'Name of the file system' },
            path: { type: 'string', description: 'Path to list (optional)', default: '' },
          },
          required: ['fileSystemName'],
        },
      },
      {
        name: 'datalake_read_file',
        description: 'Read content of a file from Data Lake',
        inputSchema: {
          type: 'object',
          properties: {
            fileSystemName: { type: 'string', description: 'Name of the file system' },
            filePath: { type: 'string', description: 'Path to the file' },
          },
          required: ['fileSystemName', 'filePath'],
        },
      },
      {
        name: 'datalake_upload_file',
        description: 'Upload a file to Data Lake',
        inputSchema: {
          type: 'object',
          properties: {
            fileSystemName: { type: 'string', description: 'Name of the file system' },
            filePath: { type: 'string', description: 'Path where to upload the file' },
            content: { type: 'string', description: 'Content of the file' },
          },
          required: ['fileSystemName', 'filePath', 'content'],
        },
      },
      {
        name: 'datalake_delete_file',
        description: 'Delete a file from Data Lake',
        inputSchema: {
          type: 'object',
          properties: {
            fileSystemName: { type: 'string', description: 'Name of the file system' },
            filePath: { type: 'string', description: 'Path to the file to delete' },
          },
          required: ['fileSystemName', 'filePath'],
        },
      },
      {
        name: 'datalake_create_directory',
        description: 'Create a directory in Data Lake',
        inputSchema: {
          type: 'object',
          properties: {
            fileSystemName: { type: 'string', description: 'Name of the file system' },
            directoryPath: { type: 'string', description: 'Path of the directory to create' },
          },
          required: ['fileSystemName', 'directoryPath'],
        },
      }
    );
  }

  // Storage Account tools
  if (storageTools) {
    tools.push(
      {
        name: 'storage_list_containers',
        description: 'List all containers in the storage account',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'storage_create_container',
        description: 'Create a new container in the storage account',
        inputSchema: {
          type: 'object',
          properties: {
            name: { type: 'string', description: 'Name of the container to create' },
            publicAccess: { 
              type: 'string', 
              enum: ['blob', 'container', null],
              description: 'Public access level (optional)' 
            },
          },
          required: ['name'],
        },
      },
      {
        name: 'storage_delete_container',
        description: 'Delete a container from the storage account',
        inputSchema: {
          type: 'object',
          properties: {
            name: { type: 'string', description: 'Name of the container to delete' },
          },
          required: ['name'],
        },
      },
      {
        name: 'storage_list_blobs',
        description: 'List blobs in a container',
        inputSchema: {
          type: 'object',
          properties: {
            containerName: { type: 'string', description: 'Name of the container' },
            prefix: { type: 'string', description: 'Filter blobs by prefix (optional)' },
          },
          required: ['containerName'],
        },
      },
      {
        name: 'storage_upload_blob',
        description: 'Upload a blob to a container',
        inputSchema: {
          type: 'object',
          properties: {
            containerName: { type: 'string', description: 'Name of the container' },
            blobName: { type: 'string', description: 'Name of the blob' },
            content: { type: 'string', description: 'Content of the blob' },
            contentType: { type: 'string', description: 'Content type (optional)' },
          },
          required: ['containerName', 'blobName', 'content'],
        },
      },
      {
        name: 'storage_download_blob',
        description: 'Download a blob from a container',
        inputSchema: {
          type: 'object',
          properties: {
            containerName: { type: 'string', description: 'Name of the container' },
            blobName: { type: 'string', description: 'Name of the blob' },
          },
          required: ['containerName', 'blobName'],
        },
      },
      {
        name: 'storage_delete_blob',
        description: 'Delete a blob from a container',
        inputSchema: {
          type: 'object',
          properties: {
            containerName: { type: 'string', description: 'Name of the container' },
            blobName: { type: 'string', description: 'Name of the blob' },
          },
          required: ['containerName', 'blobName'],
        },
      },
      {
        name: 'storage_get_blob_properties',
        description: 'Get properties of a blob',
        inputSchema: {
          type: 'object',
          properties: {
            containerName: { type: 'string', description: 'Name of the container' },
            blobName: { type: 'string', description: 'Name of the blob' },
          },
          required: ['containerName', 'blobName'],
        },
      },
      {
        name: 'storage_copy_blob',
        description: 'Copy a blob from one location to another',
        inputSchema: {
          type: 'object',
          properties: {
            sourceContainer: { type: 'string', description: 'Source container name' },
            sourceBlobName: { type: 'string', description: 'Source blob name' },
            destContainer: { type: 'string', description: 'Destination container name' },
            destBlobName: { type: 'string', description: 'Destination blob name' },
          },
          required: ['sourceContainer', 'sourceBlobName', 'destContainer', 'destBlobName'],
        },
      }
    );
  }

  // Fabric tools
  if (fabricTools) {
    tools.push(
      {
        name: 'fabric_list_workspaces',
        description: 'List all Power BI / Fabric workspaces',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'fabric_get_workspace',
        description: 'Get details of a specific workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      },
      {
        name: 'fabric_list_datasets',
        description: 'List datasets in a workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      },
      {
        name: 'fabric_list_reports',
        description: 'List reports in a workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      },
      {
        name: 'fabric_list_dashboards',
        description: 'List dashboards in a workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      },
      {
        name: 'fabric_list_dataflows',
        description: 'List dataflows in a workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      },
      {
        name: 'fabric_get_dataset',
        description: 'Get details of a specific dataset',
        inputSchema: {
          type: 'object',
          properties: {
            datasetId: { type: 'string', description: 'Dataset ID' },
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
          required: ['datasetId'],
        },
      },
      {
        name: 'fabric_refresh_dataset',
        description: 'Trigger a refresh of a dataset',
        inputSchema: {
          type: 'object',
          properties: {
            datasetId: { type: 'string', description: 'Dataset ID' },
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
          required: ['datasetId'],
        },
      },
      {
        name: 'fabric_get_dataset_refresh_history',
        description: 'Get refresh history of a dataset',
        inputSchema: {
          type: 'object',
          properties: {
            datasetId: { type: 'string', description: 'Dataset ID' },
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
          required: ['datasetId'],
        },
      },
      {
        name: 'fabric_execute_dax',
        description: 'Execute a DAX query against a dataset',
        inputSchema: {
          type: 'object',
          properties: {
            datasetId: { type: 'string', description: 'Dataset ID' },
            dax: { type: 'string', description: 'DAX query to execute' },
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
          required: ['datasetId', 'dax'],
        },
      },
      {
        name: 'fabric_list_lakehouses',
        description: 'List lakehouses in a workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      },
      {
        name: 'fabric_get_lakehouse',
        description: 'Get details of a specific lakehouse',
        inputSchema: {
          type: 'object',
          properties: {
            lakehouseId: { type: 'string', description: 'Lakehouse ID' },
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
          required: ['lakehouseId'],
        },
      },
      {
        name: 'fabric_list_notebooks',
        description: 'List notebooks in a workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Workspace ID (optional if configured)' },
          },
        },
      }
    );
  }

  // Data Factory tools
  if (dataFactoryTools) {
    tools.push(
      {
        name: 'datafactory_list_factories',
        description: 'List data factories in a resource group',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
          },
          required: ['resourceGroupName'],
        },
      },
      {
        name: 'datafactory_get_factory',
        description: 'Get details of a specific data factory',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            factoryName: { type: 'string', description: 'Data factory name' },
          },
          required: ['resourceGroupName', 'factoryName'],
        },
      },
      {
        name: 'datafactory_list_pipelines',
        description: 'List pipelines in a data factory',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            factoryName: { type: 'string', description: 'Data factory name' },
          },
          required: ['resourceGroupName', 'factoryName'],
        },
      },
      {
        name: 'datafactory_get_pipeline',
        description: 'Get details of a specific pipeline',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            factoryName: { type: 'string', description: 'Data factory name' },
            pipelineName: { type: 'string', description: 'Pipeline name' },
          },
          required: ['resourceGroupName', 'factoryName', 'pipelineName'],
        },
      },
      {
        name: 'datafactory_create_pipeline_run',
        description: 'Start a pipeline run',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            factoryName: { type: 'string', description: 'Data factory name' },
            pipelineName: { type: 'string', description: 'Pipeline name' },
            parameters: { type: 'object', description: 'Pipeline parameters (optional)' },
          },
          required: ['resourceGroupName', 'factoryName', 'pipelineName'],
        },
      },
      {
        name: 'datafactory_get_pipeline_run',
        description: 'Get pipeline run status and details',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            factoryName: { type: 'string', description: 'Data factory name' },
            runId: { type: 'string', description: 'Pipeline run ID' },
          },
          required: ['resourceGroupName', 'factoryName', 'runId'],
        },
      },
      {
        name: 'datafactory_cancel_pipeline_run',
        description: 'Cancel a running pipeline',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            factoryName: { type: 'string', description: 'Data factory name' },
            runId: { type: 'string', description: 'Pipeline run ID' },
          },
          required: ['resourceGroupName', 'factoryName', 'runId'],
        },
      }
    );
  }

  // SQL Database tools
  if (sqlDatabaseTools) {
    tools.push(
      {
        name: 'sql_list_servers',
        description: 'List SQL servers in a resource group',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
          },
          required: ['resourceGroupName'],
        },
      },
      {
        name: 'sql_get_server',
        description: 'Get details of a specific SQL server',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            serverName: { type: 'string', description: 'SQL server name' },
          },
          required: ['resourceGroupName', 'serverName'],
        },
      },
      {
        name: 'sql_list_databases',
        description: 'List databases on a SQL server',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            serverName: { type: 'string', description: 'SQL server name' },
          },
          required: ['resourceGroupName', 'serverName'],
        },
      },
      {
        name: 'sql_get_database',
        description: 'Get details of a specific database',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            serverName: { type: 'string', description: 'SQL server name' },
            databaseName: { type: 'string', description: 'Database name' },
          },
          required: ['resourceGroupName', 'serverName', 'databaseName'],
        },
      },
      {
        name: 'sql_execute_query',
        description: 'Execute a SQL query against a database',
        inputSchema: {
          type: 'object',
          properties: {
            serverName: { type: 'string', description: 'SQL server name (FQDN)' },
            databaseName: { type: 'string', description: 'Database name' },
            username: { type: 'string', description: 'Username for authentication' },
            password: { type: 'string', description: 'Password for authentication' },
            query: { type: 'string', description: 'SQL query to execute' },
          },
          required: ['serverName', 'databaseName', 'username', 'password', 'query'],
        },
      }
    );
  }

  // Databricks tools
  if (databricksTools) {
    tools.push(
      {
        name: 'databricks_list_workspaces',
        description: 'List Databricks workspaces in a resource group',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
          },
          required: ['resourceGroupName'],
        },
      },
      {
        name: 'databricks_get_workspace',
        description: 'Get details of a specific Databricks workspace',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            workspaceName: { type: 'string', description: 'Databricks workspace name' },
          },
          required: ['resourceGroupName', 'workspaceName'],
        },
      },
      {
        name: 'databricks_list_clusters',
        description: 'List clusters in the Databricks workspace',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'databricks_get_cluster',
        description: 'Get details of a specific cluster',
        inputSchema: {
          type: 'object',
          properties: {
            clusterId: { type: 'string', description: 'Cluster ID' },
          },
          required: ['clusterId'],
        },
      },
      {
        name: 'databricks_start_cluster',
        description: 'Start a Databricks cluster',
        inputSchema: {
          type: 'object',
          properties: {
            clusterId: { type: 'string', description: 'Cluster ID' },
          },
          required: ['clusterId'],
        },
      },
      {
        name: 'databricks_terminate_cluster',
        description: 'Terminate a Databricks cluster',
        inputSchema: {
          type: 'object',
          properties: {
            clusterId: { type: 'string', description: 'Cluster ID' },
          },
          required: ['clusterId'],
        },
      },
      {
        name: 'databricks_list_jobs',
        description: 'List jobs in the Databricks workspace',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'databricks_run_job',
        description: 'Run a Databricks job',
        inputSchema: {
          type: 'object',
          properties: {
            jobId: { type: 'number', description: 'Job ID' },
            notebookParams: { type: 'object', description: 'Notebook parameters (optional)' },
          },
          required: ['jobId'],
        },
      }
    );
  }

  // Monitor tools
  if (monitorTools) {
    tools.push(
      {
        name: 'monitor_list_action_groups',
        description: 'List action groups in a resource group',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
          },
          required: ['resourceGroupName'],
        },
      },
      {
        name: 'monitor_list_metric_alerts',
        description: 'List metric alerts in a resource group',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
          },
          required: ['resourceGroupName'],
        },
      },
      {
        name: 'monitor_query_logs',
        description: 'Query logs from Log Analytics workspace',
        inputSchema: {
          type: 'object',
          properties: {
            workspaceId: { type: 'string', description: 'Log Analytics workspace ID' },
            query: { type: 'string', description: 'KQL query to execute' },
            timespan: { type: 'string', description: 'Time range (optional, e.g. "PT1H" for 1 hour)' },
          },
          required: ['workspaceId', 'query'],
        },
      },
      {
        name: 'monitor_get_metrics',
        description: 'Get metrics for an Azure resource',
        inputSchema: {
          type: 'object',
          properties: {
            resourceUri: { type: 'string', description: 'Azure resource URI' },
            metricNames: { type: 'array', items: { type: 'string' }, description: 'List of metric names' },
            timespan: { type: 'string', description: 'Time range (optional)' },
            interval: { type: 'string', description: 'Metric interval (optional)' },
          },
          required: ['resourceUri', 'metricNames'],
        },
      }
    );
  }

  // Synapse tools
  if (synapseTools) {
    tools.push(
      {
        name: 'synapse_list_workspaces',
        description: 'List Synapse workspaces in a resource group',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
          },
          required: ['resourceGroupName'],
        },
      },
      {
        name: 'synapse_get_workspace',
        description: 'Get details of a specific Synapse workspace',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            workspaceName: { type: 'string', description: 'Synapse workspace name' },
          },
          required: ['resourceGroupName', 'workspaceName'],
        },
      },
      {
        name: 'synapse_list_sql_pools',
        description: 'List SQL pools in a Synapse workspace',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            workspaceName: { type: 'string', description: 'Synapse workspace name' },
          },
          required: ['resourceGroupName', 'workspaceName'],
        },
      },
      {
        name: 'synapse_pause_sql_pool',
        description: 'Pause a Synapse SQL pool',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            workspaceName: { type: 'string', description: 'Synapse workspace name' },
            sqlPoolName: { type: 'string', description: 'SQL pool name' },
          },
          required: ['resourceGroupName', 'workspaceName', 'sqlPoolName'],
        },
      },
      {
        name: 'synapse_resume_sql_pool',
        description: 'Resume a Synapse SQL pool',
        inputSchema: {
          type: 'object',
          properties: {
            resourceGroupName: { type: 'string', description: 'Resource group name' },
            workspaceName: { type: 'string', description: 'Synapse workspace name' },
            sqlPoolName: { type: 'string', description: 'SQL pool name' },
          },
          required: ['resourceGroupName', 'workspaceName', 'sqlPoolName'],
        },
      },
      {
        name: 'synapse_list_pipelines',
        description: 'List pipelines in a Synapse workspace',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'synapse_create_pipeline_run',
        description: 'Start a Synapse pipeline run',
        inputSchema: {
          type: 'object',
          properties: {
            pipelineName: { type: 'string', description: 'Pipeline name' },
            parameters: { type: 'object', description: 'Pipeline parameters (optional)' },
          },
          required: ['pipelineName'],
        },
      }
    );
  }

  // Dimensional Modeling tools
  if (dimensionalTools) {
    tools.push(
      {
        name: 'dimensional_generate_dimension',
        description: 'Generate a dimension table with SCD support',
        inputSchema: {
          type: 'object',
          properties: {
            tableName: { type: 'string', description: 'Name of the dimension table' },
            businessName: { type: 'string', description: 'Business-friendly name' },
            columns: { type: 'array', description: 'Array of dimension columns' },
            scdType: { type: 'number', enum: [1, 2, 3], description: 'SCD type (1, 2, or 3)' },
            naturalKey: { type: 'array', items: { type: 'string' }, description: 'Natural key columns' },
            description: { type: 'string', description: 'Table description' }
          },
          required: ['tableName', 'businessName', 'columns']
        }
      },
      {
        name: 'dimensional_generate_fact',
        description: 'Generate a fact table with measures and dimension keys',
        inputSchema: {
          type: 'object',
          properties: {
            tableName: { type: 'string', description: 'Name of the fact table' },
            businessName: { type: 'string', description: 'Business-friendly name' },
            measures: { type: 'array', description: 'Array of fact measures' },
            dimensionKeys: { type: 'array', items: { type: 'string' }, description: 'Dimension foreign keys' },
            grainDescription: { type: 'string', description: 'Description of the grain' },
            description: { type: 'string', description: 'Table description' }
          },
          required: ['tableName', 'businessName', 'measures', 'dimensionKeys']
        }
      },
      {
        name: 'dimensional_generate_star_schema',
        description: 'Generate a complete star schema with fact and dimension tables',
        inputSchema: {
          type: 'object',
          properties: {
            schemaName: { type: 'string', description: 'Name of the star schema' },
            factTable: { type: 'object', description: 'Fact table definition' },
            dimensionTables: { type: 'array', description: 'Array of dimension table definitions' },
            description: { type: 'string', description: 'Schema description' },
            generateViews: { type: 'boolean', description: 'Generate analytical views' }
          },
          required: ['schemaName', 'factTable', 'dimensionTables']
        }
      },
      {
        name: 'dimensional_validate_star_schema',
        description: 'Validate a star schema design for best practices',
        inputSchema: {
          type: 'object',
          properties: {
            starSchema: { type: 'object', description: 'Star schema definition to validate' }
          },
          required: ['starSchema']
        }
      }
    );
  }

  // Fabric Pipeline Orchestration tools
  if (fabricPipelineTools) {
    tools.push(
      {
        name: 'fabric_create_data_pipeline',
        description: 'Create a comprehensive data pipeline in Fabric',
        inputSchema: {
          type: 'object',
          properties: {
            pipelineDefinition: { type: 'object', description: 'Complete pipeline definition' }
          },
          required: ['pipelineDefinition']
        }
      },
      {
        name: 'fabric_create_dimension_load_pipeline',
        description: 'Create a dimension loading pipeline with SCD support',
        inputSchema: {
          type: 'object',
          properties: {
            dimensionName: { type: 'string', description: 'Dimension table name' },
            sourceDataset: { type: 'string', description: 'Source dataset name' },
            targetDataset: { type: 'string', description: 'Target dataset name' },
            naturalKeys: { type: 'array', items: { type: 'string' }, description: 'Natural key columns' },
            scdType: { type: 'number', enum: [1, 2], description: 'SCD type' }
          },
          required: ['dimensionName', 'sourceDataset', 'targetDataset', 'naturalKeys']
        }
      },
      {
        name: 'fabric_create_fact_load_pipeline',
        description: 'Create a fact table loading pipeline with dimension lookups',
        inputSchema: {
          type: 'object',
          properties: {
            factName: { type: 'string', description: 'Fact table name' },
            sourceDataset: { type: 'string', description: 'Source dataset name' },
            targetDataset: { type: 'string', description: 'Target dataset name' },
            dimensionMappings: { type: 'array', description: 'Dimension mapping definitions' }
          },
          required: ['factName', 'sourceDataset', 'targetDataset', 'dimensionMappings']
        }
      },
      {
        name: 'fabric_create_star_schema_etl',
        description: 'Create complete ETL pipeline for star schema',
        inputSchema: {
          type: 'object',
          properties: {
            starSchemaName: { type: 'string', description: 'Star schema name' },
            dimensionPipelines: { type: 'array', items: { type: 'string' }, description: 'Dimension pipeline names' },
            factPipelines: { type: 'array', items: { type: 'string' }, description: 'Fact pipeline names' }
          },
          required: ['starSchemaName', 'dimensionPipelines', 'factPipelines']
        }
      },
      {
        name: 'fabric_create_scheduled_pipeline',
        description: 'Create a scheduled pipeline with triggers',
        inputSchema: {
          type: 'object',
          properties: {
            pipelineName: { type: 'string', description: 'Pipeline name' },
            schedule: { type: 'object', description: 'Schedule configuration' }
          },
          required: ['pipelineName', 'schedule']
        }
      }
    );
  }

  // Data Warehouse Management tools
  if (dataWarehouseTools) {
    tools.push(
      {
        name: 'dw_create_architecture',
        description: 'Create complete data warehouse architecture with layers',
        inputSchema: {
          type: 'object',
          properties: {
            dwName: { type: 'string', description: 'Data warehouse name' },
            layers: { type: 'array', description: 'Array of data warehouse layers' }
          },
          required: ['dwName', 'layers']
        }
      },
      {
        name: 'dw_generate_standard_dimensions',
        description: 'Generate standard dimensions (Date, Time, Geography, Currency)',
        inputSchema: {
          type: 'object',
          properties: {},
        }
      },
      {
        name: 'dw_create_data_quality_framework',
        description: 'Create data quality framework with rules and monitoring',
        inputSchema: {
          type: 'object',
          properties: {
            rules: { type: 'array', description: 'Array of data quality rules' }
          },
          required: ['rules']
        }
      },
      {
        name: 'dw_generate_data_catalog',
        description: 'Generate comprehensive data catalog with metadata',
        inputSchema: {
          type: 'object',
          properties: {
            entries: { type: 'array', description: 'Array of catalog entries' }
          },
          required: ['entries']
        }
      },
      {
        name: 'dw_generate_data_marts',
        description: 'Generate data marts from star schemas',
        inputSchema: {
          type: 'object',
          properties: {
            starSchemas: { type: 'array', description: 'Array of star schema definitions' },
            targetDatabase: { type: 'string', description: 'Target database name' }
          },
          required: ['starSchemas']
        }
      }
    );
  }

  return { tools };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  const toolArgs = (args as Record<string, any>) || {};

  try {
    // Data Lake tools
    if (name.startsWith('datalake_') && dataLakeTools) {
      switch (name) {
        case 'datalake_list_filesystems':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.listFileSystems(), null, 2),
              },
            ],
          };

        case 'datalake_create_filesystem':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.createFileSystem(toolArgs.name), null, 2),
              },
            ],
          };

        case 'datalake_list_files':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.listFiles(toolArgs.fileSystemName, toolArgs.path), null, 2),
              },
            ],
          };

        case 'datalake_read_file':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.readFile(toolArgs.fileSystemName, toolArgs.filePath), null, 2),
              },
            ],
          };

        case 'datalake_upload_file':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.uploadFile(toolArgs.fileSystemName, toolArgs.filePath, toolArgs.content), null, 2),
              },
            ],
          };

        case 'datalake_delete_file':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.deleteFile(toolArgs.fileSystemName, toolArgs.filePath), null, 2),
              },
            ],
          };

        case 'datalake_create_directory':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataLakeTools.createDirectory(toolArgs.fileSystemName, toolArgs.directoryPath), null, 2),
              },
            ],
          };
      }
    }

    // Storage tools
    if (name.startsWith('storage_') && storageTools) {
      switch (name) {
        case 'storage_list_containers':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.listContainers(), null, 2),
              },
            ],
          };

        case 'storage_create_container':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.createContainer(toolArgs.name, toolArgs.publicAccess), null, 2),
              },
            ],
          };

        case 'storage_delete_container':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.deleteContainer(toolArgs.name), null, 2),
              },
            ],
          };

        case 'storage_list_blobs':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.listBlobs(toolArgs.containerName, toolArgs.prefix), null, 2),
              },
            ],
          };

        case 'storage_upload_blob':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.uploadBlob(toolArgs.containerName, toolArgs.blobName, toolArgs.content, toolArgs.contentType), null, 2),
              },
            ],
          };

        case 'storage_download_blob':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.downloadBlob(toolArgs.containerName, toolArgs.blobName), null, 2),
              },
            ],
          };

        case 'storage_delete_blob':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.deleteBlob(toolArgs.containerName, toolArgs.blobName), null, 2),
              },
            ],
          };

        case 'storage_get_blob_properties':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.getBlobProperties(toolArgs.containerName, toolArgs.blobName), null, 2),
              },
            ],
          };

        case 'storage_copy_blob':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await storageTools.copyBlob(toolArgs.sourceContainer, toolArgs.sourceBlobName, toolArgs.destContainer, toolArgs.destBlobName), null, 2),
              },
            ],
          };
      }
    }

    // Fabric tools
    if (name.startsWith('fabric_') && fabricTools) {
      switch (name) {
        case 'fabric_list_workspaces':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listWorkspaces(), null, 2),
              },
            ],
          };

        case 'fabric_get_workspace':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.getWorkspace(toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_list_datasets':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listDatasets(toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_list_reports':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listReports(toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_list_dashboards':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listDashboards(toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_list_dataflows':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listDataflows(toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_get_dataset':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.getDataset(toolArgs.datasetId, toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_refresh_dataset':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.refreshDataset(toolArgs.datasetId, toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_get_dataset_refresh_history':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.getDatasetRefreshHistory(toolArgs.datasetId, toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_execute_dax':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.executeDAX(toolArgs.datasetId, toolArgs.dax, toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_list_lakehouses':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listLakehouses(toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_get_lakehouse':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.getLakehouse(toolArgs.lakehouseId, toolArgs.workspaceId), null, 2),
              },
            ],
          };

        case 'fabric_list_notebooks':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricTools.listNotebooks(toolArgs.workspaceId), null, 2),
              },
            ],
          };
      }
    }

    // Data Factory tools
    if (name.startsWith('datafactory_') && dataFactoryTools) {
      switch (name) {
        case 'datafactory_list_factories':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.listFactories(toolArgs.resourceGroupName), null, 2),
              },
            ],
          };

        case 'datafactory_get_factory':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.getFactory(toolArgs.resourceGroupName, toolArgs.factoryName), null, 2),
              },
            ],
          };

        case 'datafactory_list_pipelines':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.listPipelines(toolArgs.resourceGroupName, toolArgs.factoryName), null, 2),
              },
            ],
          };

        case 'datafactory_get_pipeline':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.getPipeline(toolArgs.resourceGroupName, toolArgs.factoryName, toolArgs.pipelineName), null, 2),
              },
            ],
          };

        case 'datafactory_create_pipeline_run':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.createPipelineRun(toolArgs.resourceGroupName, toolArgs.factoryName, toolArgs.pipelineName, toolArgs.parameters), null, 2),
              },
            ],
          };

        case 'datafactory_get_pipeline_run':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.getPipelineRun(toolArgs.resourceGroupName, toolArgs.factoryName, toolArgs.runId), null, 2),
              },
            ],
          };

        case 'datafactory_cancel_pipeline_run':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataFactoryTools.cancelPipelineRun(toolArgs.resourceGroupName, toolArgs.factoryName, toolArgs.runId), null, 2),
              },
            ],
          };
      }
    }

    // SQL Database tools
    if (name.startsWith('sql_') && sqlDatabaseTools) {
      switch (name) {
        case 'sql_list_servers':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await sqlDatabaseTools.listServers(toolArgs.resourceGroupName), null, 2),
              },
            ],
          };

        case 'sql_get_server':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await sqlDatabaseTools.getServer(toolArgs.resourceGroupName, toolArgs.serverName), null, 2),
              },
            ],
          };

        case 'sql_list_databases':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await sqlDatabaseTools.listDatabases(toolArgs.resourceGroupName, toolArgs.serverName), null, 2),
              },
            ],
          };

        case 'sql_get_database':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await sqlDatabaseTools.getDatabase(toolArgs.resourceGroupName, toolArgs.serverName, toolArgs.databaseName), null, 2),
              },
            ],
          };

        case 'sql_execute_query':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await sqlDatabaseTools.executeQuery(toolArgs.serverName, toolArgs.databaseName, toolArgs.username, toolArgs.password, toolArgs.query), null, 2),
              },
            ],
          };
      }
    }

    // Databricks tools
    if (name.startsWith('databricks_') && databricksTools) {
      switch (name) {
        case 'databricks_list_workspaces':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.listWorkspaces(toolArgs.resourceGroupName), null, 2),
              },
            ],
          };

        case 'databricks_get_workspace':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.getWorkspace(toolArgs.resourceGroupName, toolArgs.workspaceName), null, 2),
              },
            ],
          };

        case 'databricks_list_clusters':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.listClusters(), null, 2),
              },
            ],
          };

        case 'databricks_get_cluster':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.getCluster(toolArgs.clusterId), null, 2),
              },
            ],
          };

        case 'databricks_start_cluster':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.startCluster(toolArgs.clusterId), null, 2),
              },
            ],
          };

        case 'databricks_terminate_cluster':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.terminateCluster(toolArgs.clusterId), null, 2),
              },
            ],
          };

        case 'databricks_list_jobs':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.listJobs(), null, 2),
              },
            ],
          };

        case 'databricks_run_job':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await databricksTools.runJobNow(toolArgs.jobId, toolArgs.notebookParams), null, 2),
              },
            ],
          };
      }
    }

    // Monitor tools
    if (name.startsWith('monitor_') && monitorTools) {
      switch (name) {
        case 'monitor_list_action_groups':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await monitorTools.listActionGroups(toolArgs.resourceGroupName), null, 2),
              },
            ],
          };

        case 'monitor_list_metric_alerts':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await monitorTools.listMetricAlerts(toolArgs.resourceGroupName), null, 2),
              },
            ],
          };

        case 'monitor_query_logs':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await monitorTools.queryLogs(toolArgs.workspaceId, toolArgs.query, toolArgs.timespan), null, 2),
              },
            ],
          };

        case 'monitor_get_metrics':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await monitorTools.getMetrics(toolArgs.resourceUri, toolArgs.metricNames, toolArgs.timespan, toolArgs.interval), null, 2),
              },
            ],
          };
      }
    }

    // Synapse tools
    if (name.startsWith('synapse_') && synapseTools) {
      switch (name) {
        case 'synapse_list_workspaces':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.listWorkspaces(toolArgs.resourceGroupName), null, 2),
              },
            ],
          };

        case 'synapse_get_workspace':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.getWorkspace(toolArgs.resourceGroupName, toolArgs.workspaceName), null, 2),
              },
            ],
          };

        case 'synapse_list_sql_pools':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.listSqlPools(toolArgs.resourceGroupName, toolArgs.workspaceName), null, 2),
              },
            ],
          };

        case 'synapse_pause_sql_pool':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.pauseSqlPool(toolArgs.resourceGroupName, toolArgs.workspaceName, toolArgs.sqlPoolName), null, 2),
              },
            ],
          };

        case 'synapse_resume_sql_pool':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.resumeSqlPool(toolArgs.resourceGroupName, toolArgs.workspaceName, toolArgs.sqlPoolName), null, 2),
              },
            ],
          };

        case 'synapse_list_pipelines':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.listPipelines(), null, 2),
              },
            ],
          };

        case 'synapse_create_pipeline_run':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await synapseTools.createPipelineRun(toolArgs.pipelineName, toolArgs.parameters), null, 2),
              },
            ],
          };
      }
    }

    // Dimensional Modeling tools
    if (name.startsWith('dimensional_') && dimensionalTools) {
      switch (name) {
        case 'dimensional_generate_dimension':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dimensionalTools.generateDimensionTable(
                  toolArgs.tableName,
                  toolArgs.businessName,
                  toolArgs.columns,
                  {
                    scdType: toolArgs.scdType,
                    naturalKey: toolArgs.naturalKey,
                    description: toolArgs.description
                  }
                ), null, 2),
              },
            ],
          };

        case 'dimensional_generate_fact':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dimensionalTools.generateFactTable(
                  toolArgs.tableName,
                  toolArgs.businessName,
                  toolArgs.measures,
                  toolArgs.dimensionKeys,
                  {
                    grainDescription: toolArgs.grainDescription,
                    description: toolArgs.description
                  }
                ), null, 2),
              },
            ],
          };

        case 'dimensional_generate_star_schema':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dimensionalTools.generateStarSchema(
                  toolArgs.schemaName,
                  toolArgs.factTable,
                  toolArgs.dimensionTables,
                  {
                    description: toolArgs.description,
                    generateViews: toolArgs.generateViews
                  }
                ), null, 2),
              },
            ],
          };

        case 'dimensional_validate_star_schema':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dimensionalTools.validateStarSchema(toolArgs.starSchema), null, 2),
              },
            ],
          };
      }
    }

    // Fabric Pipeline Orchestration tools
    if (name.startsWith('fabric_create_') && fabricPipelineTools) {
      switch (name) {
        case 'fabric_create_data_pipeline':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricPipelineTools.createDataPipeline(toolArgs.pipelineDefinition), null, 2),
              },
            ],
          };

        case 'fabric_create_dimension_load_pipeline':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricPipelineTools.createDimensionLoadPipeline(
                  toolArgs.dimensionName,
                  toolArgs.sourceDataset,
                  toolArgs.targetDataset,
                  toolArgs.naturalKeys,
                  toolArgs.scdType
                ), null, 2),
              },
            ],
          };

        case 'fabric_create_fact_load_pipeline':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricPipelineTools.createFactLoadPipeline(
                  toolArgs.factName,
                  toolArgs.sourceDataset,
                  toolArgs.targetDataset,
                  toolArgs.dimensionMappings
                ), null, 2),
              },
            ],
          };

        case 'fabric_create_star_schema_etl':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricPipelineTools.createStarSchemaETLPipeline(
                  toolArgs.starSchemaName,
                  toolArgs.dimensionPipelines,
                  toolArgs.factPipelines
                ), null, 2),
              },
            ],
          };

        case 'fabric_create_scheduled_pipeline':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await fabricPipelineTools.createScheduledPipeline(
                  toolArgs.pipelineName,
                  toolArgs.schedule
                ), null, 2),
              },
            ],
          };
      }
    }

    // Data Warehouse Management tools
    if (name.startsWith('dw_') && dataWarehouseTools) {
      switch (name) {
        case 'dw_create_architecture':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataWarehouseTools.createDataWarehouseArchitecture(
                  toolArgs.dwName,
                  toolArgs.layers
                ), null, 2),
              },
            ],
          };

        case 'dw_generate_standard_dimensions':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataWarehouseTools.generateStandardDimensions(), null, 2),
              },
            ],
          };

        case 'dw_create_data_quality_framework':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataWarehouseTools.createDataQualityFramework(toolArgs.rules), null, 2),
              },
            ],
          };

        case 'dw_generate_data_catalog':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataWarehouseTools.generateDataCatalog(toolArgs.entries), null, 2),
              },
            ],
          };

        case 'dw_generate_data_marts':
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(await dataWarehouseTools.generateDataMarts(
                  toolArgs.starSchemas,
                  toolArgs.targetDatabase
                ), null, 2),
              },
            ],
          };
      }
    }

    throw new Error(`Unknown tool: ${name}`);
  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `Error: ${error instanceof Error ? error.message : String(error)}`,
        },
      ],
      isError: true,
    };
  }
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('Azure MCP Server running on stdio');
}

main().catch((error) => {
  console.error('Fatal error in main():', error);
  process.exit(1);
});