import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

// Note: Azure Fabric SDK is still evolving, so we'll use REST API approach
export class FabricTools {
  private baseUrl = 'https://api.fabric.microsoft.com/v1';
  private credential: any;
  
  constructor(private config: AzureConfig) {
    this.credential = getAzureCredential(config);
  }

  private async getAccessToken(): Promise<string> {
    try {
      const tokenResponse = await this.credential.getToken('https://api.fabric.microsoft.com/.default');
      return tokenResponse.token;
    } catch (error) {
      throw new Error(`Failed to get access token: ${error}`);
    }
  }

  private async makeRequest(endpoint: string, method: 'GET' | 'POST' | 'PUT' | 'DELETE' = 'GET', body?: any) {
    try {
      const token = await this.getAccessToken();
      const response = await fetch(`${this.baseUrl}${endpoint}`, {
        method,
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: body ? JSON.stringify(body) : undefined
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      throw new Error(`API request failed: ${error}`);
    }
  }

  async listWorkspaces() {
    try {
      const response = await this.makeRequest('/workspaces');
      return {
        workspaces: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list workspaces: ${error}`);
    }
  }

  async getWorkspace(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}`);
      return response;
    } catch (error) {
      throw new Error(`Failed to get workspace: ${error}`);
    }
  }

  async listDatasets(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/datasets`);
      return {
        datasets: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list datasets: ${error}`);
    }
  }

  async listReports(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/reports`);
      return {
        reports: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list reports: ${error}`);
    }
  }

  async listDashboards(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/dashboards`);
      return {
        dashboards: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list dashboards: ${error}`);
    }
  }

  async listDataflows(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/dataflows`);
      return {
        dataflows: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list dataflows: ${error}`);
    }
  }

  async getDataset(datasetId: string, workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/datasets/${datasetId}`);
      return response;
    } catch (error) {
      throw new Error(`Failed to get dataset: ${error}`);
    }
  }

  async refreshDataset(datasetId: string, workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      await this.makeRequest(`/workspaces/${id}/datasets/${datasetId}/refreshes`, 'POST');
      return { message: `Dataset '${datasetId}' refresh initiated successfully` };
    } catch (error) {
      throw new Error(`Failed to refresh dataset: ${error}`);
    }
  }

  async getDatasetRefreshHistory(datasetId: string, workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/datasets/${datasetId}/refreshes`);
      return {
        refreshes: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to get dataset refresh history: ${error}`);
    }
  }

  async executeDAX(datasetId: string, dax: string, workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(
        `/workspaces/${id}/datasets/${datasetId}/executeQueries`,
        'POST',
        {
          queries: [{
            query: dax
          }]
        }
      );
      
      return {
        results: response.results || []
      };
    } catch (error) {
      throw new Error(`Failed to execute DAX query: ${error}`);
    }
  }

  async listLakehouses(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/lakehouses`);
      return {
        lakehouses: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list lakehouses: ${error}`);
    }
  }

  async getLakehouse(lakehouseId: string, workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/lakehouses/${lakehouseId}`);
      return response;
    } catch (error) {
      throw new Error(`Failed to get lakehouse: ${error}`);
    }
  }

  async listNotebooks(workspaceId?: string) {
    try {
      const id = workspaceId || this.config.fabricWorkspaceId;
      if (!id) {
        throw new Error('Workspace ID is required');
      }
      
      const response = await this.makeRequest(`/workspaces/${id}/notebooks`);
      return {
        notebooks: response.value || []
      };
    } catch (error) {
      throw new Error(`Failed to list notebooks: ${error}`);
    }
  }
}