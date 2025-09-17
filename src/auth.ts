import { DefaultAzureCredential, ClientSecretCredential } from '@azure/identity';
import { AzureConfig } from './config.js';

export function getAzureCredential(config: AzureConfig) {
  if (config.clientId && config.clientSecret && config.tenantId) {
    return new ClientSecretCredential(
      config.tenantId,
      config.clientId,
      config.clientSecret
    );
  }
  
  return new DefaultAzureCredential();
}