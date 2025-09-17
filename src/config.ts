import { z } from 'zod';

export const AzureConfigSchema = z.object({
  subscriptionId: z.string().optional(),
  tenantId: z.string().optional(),
  clientId: z.string().optional(),
  clientSecret: z.string().optional(),
  storageAccountName: z.string().optional(),
  storageAccountKey: z.string().optional(),
  dataLakeAccountName: z.string().optional(),
  fabricWorkspaceId: z.string().optional(),
  databricksToken: z.string().optional(),
  databricksWorkspaceUrl: z.string().optional(),
  synapseToken: z.string().optional(),
  synapseWorkspaceUrl: z.string().optional(),
  logAnalyticsWorkspaceId: z.string().optional(),
  fabricToken: z.string().optional(),
  fabricWorkspaceUrl: z.string().optional(),
});

export type AzureConfig = z.infer<typeof AzureConfigSchema>;

export function getConfig(): AzureConfig {
  return {
    subscriptionId: process.env.AZURE_SUBSCRIPTION_ID,
    tenantId: process.env.AZURE_TENANT_ID,
    clientId: process.env.AZURE_CLIENT_ID,
    clientSecret: process.env.AZURE_CLIENT_SECRET,
    storageAccountName: process.env.AZURE_STORAGE_ACCOUNT_NAME,
    storageAccountKey: process.env.AZURE_STORAGE_ACCOUNT_KEY,
    dataLakeAccountName: process.env.AZURE_DATALAKE_ACCOUNT_NAME,
    fabricWorkspaceId: process.env.AZURE_FABRIC_WORKSPACE_ID,
    databricksToken: process.env.AZURE_DATABRICKS_TOKEN,
    databricksWorkspaceUrl: process.env.AZURE_DATABRICKS_WORKSPACE_URL,
    synapseToken: process.env.AZURE_SYNAPSE_TOKEN,
    synapseWorkspaceUrl: process.env.AZURE_SYNAPSE_WORKSPACE_URL,
    logAnalyticsWorkspaceId: process.env.AZURE_LOG_ANALYTICS_WORKSPACE_ID,
    fabricToken: process.env.AZURE_FABRIC_TOKEN,
    fabricWorkspaceUrl: process.env.AZURE_FABRIC_WORKSPACE_URL,
  };
}