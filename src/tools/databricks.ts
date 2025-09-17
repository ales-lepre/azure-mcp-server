import { AzureDatabricksManagementClient } from '@azure/arm-databricks';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class DatabricksTools {
  private managementClient: AzureDatabricksManagementClient;
  private databricksToken?: string;
  private workspaceUrl?: string;

  constructor(private config: AzureConfig & { databricksToken?: string; databricksWorkspaceUrl?: string }) {
    if (!config.subscriptionId) {
      throw new Error('Subscription ID is required for Databricks operations');
    }

    const credential = getAzureCredential(config);
    this.managementClient = new AzureDatabricksManagementClient(credential, config.subscriptionId);
    this.databricksToken = config.databricksToken;
    this.workspaceUrl = config.databricksWorkspaceUrl;
  }

  async listWorkspaces(resourceGroupName: string) {
    try {
      const workspaces = [];
      for await (const workspace of this.managementClient.workspaces.listByResourceGroup(resourceGroupName)) {
        workspaces.push({
          name: workspace.name,
          location: workspace.location,
          provisioningState: workspace.provisioningState,
          workspaceUrl: workspace.workspaceUrl,
          workspaceId: workspace.workspaceId,
          managedResourceGroupId: workspace.managedResourceGroupId,
          sku: workspace.sku,
          authorizations: workspace.authorizations,
          storageAccountIdentity: workspace.storageAccountIdentity
        });
      }
      return { workspaces };
    } catch (error) {
      throw new Error(`Failed to list Databricks workspaces: ${error}`);
    }
  }

  async getWorkspace(resourceGroupName: string, workspaceName: string) {
    try {
      const workspace = await this.managementClient.workspaces.get(resourceGroupName, workspaceName);
      return {
        name: workspace.name,
        location: workspace.location,
        provisioningState: workspace.provisioningState,
        workspaceUrl: workspace.workspaceUrl,
        workspaceId: workspace.workspaceId,
        managedResourceGroupId: workspace.managedResourceGroupId,
        sku: workspace.sku,
        parameters: workspace.parameters,
        authorizations: workspace.authorizations,
        createdDateTime: workspace.createdDateTime,
        storageAccountIdentity: workspace.storageAccountIdentity,
        tags: workspace.tags
      };
    } catch (error) {
      throw new Error(`Failed to get Databricks workspace: ${error}`);
    }
  }

  private async makeApiRequest(endpoint: string, method: string = 'GET', body?: any) {
    if (!this.databricksToken || !this.workspaceUrl) {
      throw new Error('Databricks token and workspace URL are required for API operations');
    }

    const url = `${this.workspaceUrl}/api/2.0/${endpoint}`;
    const options: RequestInit = {
      method,
      headers: {
        'Authorization': `Bearer ${this.databricksToken}`,
        'Content-Type': 'application/json'
      }
    };

    if (body && method !== 'GET') {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Databricks API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
  }

  async listClusters() {
    try {
      const response = await this.makeApiRequest('clusters/list');
      return {
        clusters: response.clusters?.map((cluster: any) => ({
          clusterId: cluster.cluster_id,
          clusterName: cluster.cluster_name,
          sparkVersion: cluster.spark_version,
          nodeTypeId: cluster.node_type_id,
          driverNodeTypeId: cluster.driver_node_type_id,
          numWorkers: cluster.num_workers,
          autoScale: cluster.autoscale,
          state: cluster.state,
          stateMessage: cluster.state_message,
          startTime: cluster.start_time,
          terminateTime: cluster.terminate_time,
          lastStateLossTime: cluster.last_state_loss_time,
          lastActivityTime: cluster.last_activity_time,
          clusterMemoryMb: cluster.cluster_memory_mb,
          clusterCores: cluster.cluster_cores,
          defaultTags: cluster.default_tags,
          creatorUserName: cluster.creator_user_name
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list clusters: ${error}`);
    }
  }

  async getCluster(clusterId: string) {
    try {
      const response = await this.makeApiRequest(`clusters/get?cluster_id=${clusterId}`);
      return {
        clusterId: response.cluster_id,
        clusterName: response.cluster_name,
        sparkVersion: response.spark_version,
        nodeTypeId: response.node_type_id,
        driverNodeTypeId: response.driver_node_type_id,
        numWorkers: response.num_workers,
        autoScale: response.autoscale,
        state: response.state,
        stateMessage: response.state_message,
        startTime: response.start_time,
        terminateTime: response.terminate_time,
        lastStateLossTime: response.last_state_loss_time,
        lastActivityTime: response.last_activity_time,
        clusterMemoryMb: response.cluster_memory_mb,
        clusterCores: response.cluster_cores,
        defaultTags: response.default_tags,
        creatorUserName: response.creator_user_name,
        sparkConf: response.spark_conf,
        awsAttributes: response.aws_attributes,
        customTags: response.custom_tags,
        clusterLogConf: response.cluster_log_conf,
        initScripts: response.init_scripts,
        sshPublicKeys: response.ssh_public_keys,
        enableElasticDisk: response.enable_elastic_disk
      };
    } catch (error) {
      throw new Error(`Failed to get cluster: ${error}`);
    }
  }

  async startCluster(clusterId: string) {
    try {
      await this.makeApiRequest('clusters/start', 'POST', { cluster_id: clusterId });
      return { message: `Cluster ${clusterId} start initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to start cluster: ${error}`);
    }
  }

  async terminateCluster(clusterId: string) {
    try {
      await this.makeApiRequest('clusters/delete', 'POST', { cluster_id: clusterId });
      return { message: `Cluster ${clusterId} termination initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to terminate cluster: ${error}`);
    }
  }

  async listJobs() {
    try {
      const response = await this.makeApiRequest('jobs/list');
      return {
        jobs: response.jobs?.map((job: any) => ({
          jobId: job.job_id,
          creatorUserName: job.creator_user_name,
          runAsUserName: job.run_as_user_name,
          settings: {
            name: job.settings?.name,
            tags: job.settings?.tags,
            tasks: job.settings?.tasks,
            schedule: job.settings?.schedule,
            maxConcurrentRuns: job.settings?.max_concurrent_runs,
            timeoutSeconds: job.settings?.timeout_seconds,
            emailNotifications: job.settings?.email_notifications,
            webhookNotifications: job.settings?.webhook_notifications
          },
          createdTime: job.created_time
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list jobs: ${error}`);
    }
  }

  async getJob(jobId: number) {
    try {
      const response = await this.makeApiRequest(`jobs/get?job_id=${jobId}`);
      return {
        jobId: response.job_id,
        creatorUserName: response.creator_user_name,
        runAsUserName: response.run_as_user_name,
        settings: response.settings,
        createdTime: response.created_time
      };
    } catch (error) {
      throw new Error(`Failed to get job: ${error}`);
    }
  }

  async runJobNow(jobId: number, notebookParams?: Record<string, string>, jarParams?: string[], pythonParams?: string[]) {
    try {
      const requestBody: any = { job_id: jobId };

      if (notebookParams) requestBody.notebook_params = notebookParams;
      if (jarParams) requestBody.jar_params = jarParams;
      if (pythonParams) requestBody.python_params = pythonParams;

      const response = await this.makeApiRequest('jobs/run-now', 'POST', requestBody);
      return {
        runId: response.run_id,
        message: `Job ${jobId} run initiated successfully`
      };
    } catch (error) {
      throw new Error(`Failed to run job: ${error}`);
    }
  }

  async getJobRun(runId: number) {
    try {
      const response = await this.makeApiRequest(`jobs/runs/get?run_id=${runId}`);
      return {
        runId: response.run_id,
        jobId: response.job_id,
        creatorUserName: response.creator_user_name,
        runAsUserName: response.run_as_user_name,
        state: response.state,
        schedule: response.schedule,
        tasks: response.tasks,
        startTime: response.start_time,
        setupDuration: response.setup_duration,
        executionDuration: response.execution_duration,
        cleanupDuration: response.cleanup_duration,
        endTime: response.end_time,
        runType: response.run_type
      };
    } catch (error) {
      throw new Error(`Failed to get job run: ${error}`);
    }
  }

  async cancelJobRun(runId: number) {
    try {
      await this.makeApiRequest('jobs/runs/cancel', 'POST', { run_id: runId });
      return { message: `Job run ${runId} cancellation initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to cancel job run: ${error}`);
    }
  }

  async listNotebooks(path: string = '/') {
    try {
      const response = await this.makeApiRequest(`workspace/list?path=${encodeURIComponent(path)}`);
      return {
        objects: response.objects?.map((obj: any) => ({
          path: obj.path,
          objectType: obj.object_type,
          objectId: obj.object_id,
          language: obj.language,
          modifiedAt: obj.modified_at,
          createdAt: obj.created_at
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list notebooks: ${error}`);
    }
  }

  async exportNotebook(path: string, format: 'SOURCE' | 'HTML' | 'JUPYTER' | 'DBC' = 'SOURCE') {
    try {
      const response = await this.makeApiRequest(`workspace/export?path=${encodeURIComponent(path)}&format=${format}`);
      return {
        content: response.content,
        path: path,
        format: format
      };
    } catch (error) {
      throw new Error(`Failed to export notebook: ${error}`);
    }
  }

  async listDbfs(path: string = '/') {
    try {
      const response = await this.makeApiRequest(`dbfs/list?path=${encodeURIComponent(path)}`);
      return {
        files: response.files?.map((file: any) => ({
          path: file.path,
          isDir: file.is_dir,
          fileSize: file.file_size,
          modificationTime: file.modification_time
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list DBFS: ${error}`);
    }
  }

  async getDbfsStatus(path: string) {
    try {
      const response = await this.makeApiRequest(`dbfs/get-status?path=${encodeURIComponent(path)}`);
      return {
        path: response.path,
        isDir: response.is_dir,
        fileSize: response.file_size,
        modificationTime: response.modification_time
      };
    } catch (error) {
      throw new Error(`Failed to get DBFS file status: ${error}`);
    }
  }
}