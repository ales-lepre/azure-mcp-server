import { DataFactoryManagementClient } from '@azure/arm-datafactory';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class DataFactoryTools {
  private client: DataFactoryManagementClient;

  constructor(private config: AzureConfig) {
    if (!config.subscriptionId) {
      throw new Error('Subscription ID is required for Data Factory operations');
    }

    const credential = getAzureCredential(config);
    this.client = new DataFactoryManagementClient(credential, config.subscriptionId);
  }

  async listFactories(resourceGroupName: string) {
    try {
      const factories = [];
      for await (const factory of this.client.factories.listByResourceGroup(resourceGroupName)) {
        factories.push({
          name: factory.name,
          location: factory.location,
          provisioningState: factory.provisioningState,
          repoConfiguration: factory.repoConfiguration,
          identity: factory.identity
        });
      }
      return { factories };
    } catch (error) {
      throw new Error(`Failed to list data factories: ${error}`);
    }
  }

  async getFactory(resourceGroupName: string, factoryName: string) {
    try {
      const factory = await this.client.factories.get(resourceGroupName, factoryName);
      return {
        name: factory.name,
        location: factory.location,
        provisioningState: factory.provisioningState,
        repoConfiguration: factory.repoConfiguration,
        identity: factory.identity,
        createTime: factory.createTime,
        version: factory.version
      };
    } catch (error) {
      throw new Error(`Failed to get data factory: ${error}`);
    }
  }

  async listPipelines(resourceGroupName: string, factoryName: string) {
    try {
      const pipelines = [];
      for await (const pipeline of this.client.pipelines.listByFactory(resourceGroupName, factoryName)) {
        pipelines.push({
          name: pipeline.name,
          type: pipeline.type,
          etag: pipeline.etag,
          description: pipeline.description
        });
      }
      return { pipelines };
    } catch (error) {
      throw new Error(`Failed to list pipelines: ${error}`);
    }
  }

  async getPipeline(resourceGroupName: string, factoryName: string, pipelineName: string) {
    try {
      const pipeline = await this.client.pipelines.get(resourceGroupName, factoryName, pipelineName);
      return {
        name: pipeline.name,
        type: pipeline.type,
        description: pipeline.description,
        activities: pipeline.activities,
        parameters: pipeline.parameters,
        variables: pipeline.variables,
        annotations: pipeline.annotations,
        runDimensions: pipeline.runDimensions
      };
    } catch (error) {
      throw new Error(`Failed to get pipeline: ${error}`);
    }
  }

  async createPipelineRun(resourceGroupName: string, factoryName: string, pipelineName: string, parameters?: Record<string, any>) {
    try {
      const runResponse = await this.client.pipelines.createRun(
        resourceGroupName,
        factoryName,
        pipelineName,
        {
          parameters: parameters
        }
      );
      return {
        runId: runResponse.runId,
        message: `Pipeline run started successfully`
      };
    } catch (error) {
      throw new Error(`Failed to create pipeline run: ${error}`);
    }
  }

  async getPipelineRun(resourceGroupName: string, factoryName: string, runId: string) {
    try {
      const run = await this.client.pipelineRuns.get(resourceGroupName, factoryName, runId);
      return {
        runId: run.runId,
        pipelineName: run.pipelineName,
        status: run.status,
        runStart: run.runStart,
        runEnd: run.runEnd,
        durationInMs: run.durationInMs,
        parameters: run.parameters,
        message: run.message,
        lastUpdated: run.lastUpdated,
        invokedBy: run.invokedBy
      };
    } catch (error) {
      throw new Error(`Failed to get pipeline run: ${error}`);
    }
  }

  async listPipelineRuns(resourceGroupName: string, factoryName: string, lastUpdatedAfter: string, lastUpdatedBefore: string) {
    try {
      const runs = await this.client.pipelineRuns.queryByFactory(
        resourceGroupName,
        factoryName,
        {
          lastUpdatedAfter: new Date(lastUpdatedAfter),
          lastUpdatedBefore: new Date(lastUpdatedBefore)
        }
      );
      return {
        runs: runs.value?.map(run => ({
          runId: run.runId,
          pipelineName: run.pipelineName,
          status: run.status,
          runStart: run.runStart,
          runEnd: run.runEnd,
          durationInMs: run.durationInMs,
          message: run.message,
          lastUpdated: run.lastUpdated
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list pipeline runs: ${error}`);
    }
  }

  async cancelPipelineRun(resourceGroupName: string, factoryName: string, runId: string) {
    try {
      await this.client.pipelineRuns.cancel(resourceGroupName, factoryName, runId);
      return { message: `Pipeline run ${runId} cancelled successfully` };
    } catch (error) {
      throw new Error(`Failed to cancel pipeline run: ${error}`);
    }
  }

  async listDatasets(resourceGroupName: string, factoryName: string) {
    try {
      const datasets = [];
      for await (const dataset of this.client.datasets.listByFactory(resourceGroupName, factoryName)) {
        datasets.push({
          name: dataset.name,
          type: dataset.type,
          etag: dataset.etag
        });
      }
      return { datasets };
    } catch (error) {
      throw new Error(`Failed to list datasets: ${error}`);
    }
  }

  async getDataset(resourceGroupName: string, factoryName: string, datasetName: string) {
    try {
      const dataset = await this.client.datasets.get(resourceGroupName, factoryName, datasetName);
      return {
        name: dataset.name,
        type: dataset.type,
        properties: dataset.properties,
        etag: dataset.etag
      };
    } catch (error) {
      throw new Error(`Failed to get dataset: ${error}`);
    }
  }

  async listLinkedServices(resourceGroupName: string, factoryName: string) {
    try {
      const linkedServices = [];
      for await (const service of this.client.linkedServices.listByFactory(resourceGroupName, factoryName)) {
        linkedServices.push({
          name: service.name,
          type: service.type,
          etag: service.etag
        });
      }
      return { linkedServices };
    } catch (error) {
      throw new Error(`Failed to list linked services: ${error}`);
    }
  }

  async getLinkedService(resourceGroupName: string, factoryName: string, linkedServiceName: string) {
    try {
      const service = await this.client.linkedServices.get(resourceGroupName, factoryName, linkedServiceName);
      return {
        name: service.name,
        type: service.type,
        properties: service.properties,
        etag: service.etag
      };
    } catch (error) {
      throw new Error(`Failed to get linked service: ${error}`);
    }
  }

  async listTriggers(resourceGroupName: string, factoryName: string) {
    try {
      const triggers = [];
      for await (const trigger of this.client.triggers.listByFactory(resourceGroupName, factoryName)) {
        triggers.push({
          name: trigger.name,
          type: trigger.type,
          etag: trigger.etag
        });
      }
      return { triggers };
    } catch (error) {
      throw new Error(`Failed to list triggers: ${error}`);
    }
  }

  async getTrigger(resourceGroupName: string, factoryName: string, triggerName: string) {
    try {
      const trigger = await this.client.triggers.get(resourceGroupName, factoryName, triggerName);
      return {
        name: trigger.name,
        type: trigger.type,
        properties: trigger.properties,
        etag: trigger.etag
      };
    } catch (error) {
      throw new Error(`Failed to get trigger: ${error}`);
    }
  }

  async startTrigger(resourceGroupName: string, factoryName: string, triggerName: string) {
    try {
      await this.client.triggers.beginStart(resourceGroupName, factoryName, triggerName);
      return { message: `Trigger ${triggerName} start initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to start trigger: ${error}`);
    }
  }

  async stopTrigger(resourceGroupName: string, factoryName: string, triggerName: string) {
    try {
      await this.client.triggers.beginStop(resourceGroupName, factoryName, triggerName);
      return { message: `Trigger ${triggerName} stop initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to stop trigger: ${error}`);
    }
  }
}