import { SqlManagementClient } from '@azure/arm-sql';
import { Connection, Request } from 'tedious';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class SqlDatabaseTools {
  private managementClient: SqlManagementClient;

  constructor(private config: AzureConfig) {
    if (!config.subscriptionId) {
      throw new Error('Subscription ID is required for SQL Database operations');
    }

    const credential = getAzureCredential(config);
    this.managementClient = new SqlManagementClient(credential, config.subscriptionId);
  }

  async listServers(resourceGroupName: string) {
    try {
      const servers = [];
      for await (const server of this.managementClient.servers.listByResourceGroup(resourceGroupName)) {
        servers.push({
          name: server.name,
          location: server.location,
          state: server.state,
          fullyQualifiedDomainName: server.fullyQualifiedDomainName,
          administratorLogin: server.administratorLogin,
          version: server.version,
          minimalTlsVersion: server.minimalTlsVersion,
          publicNetworkAccess: server.publicNetworkAccess
        });
      }
      return { servers };
    } catch (error) {
      throw new Error(`Failed to list SQL servers: ${error}`);
    }
  }

  async getServer(resourceGroupName: string, serverName: string) {
    try {
      const server = await this.managementClient.servers.get(resourceGroupName, serverName);
      return {
        name: server.name,
        location: server.location,
        state: server.state,
        fullyQualifiedDomainName: server.fullyQualifiedDomainName,
        administratorLogin: server.administratorLogin,
        version: server.version,
        minimalTlsVersion: server.minimalTlsVersion,
        publicNetworkAccess: server.publicNetworkAccess,
        tags: server.tags
      };
    } catch (error) {
      throw new Error(`Failed to get SQL server: ${error}`);
    }
  }

  async listDatabases(resourceGroupName: string, serverName: string) {
    try {
      const databases = [];
      for await (const database of this.managementClient.databases.listByServer(resourceGroupName, serverName)) {
        databases.push({
          name: database.name,
          location: database.location,
          status: database.status,
          collation: database.collation,
          maxSizeBytes: database.maxSizeBytes,
          requestedServiceObjectiveName: database.requestedServiceObjectiveName,
          creationDate: database.creationDate,
          earliestRestoreDate: database.earliestRestoreDate
        });
      }
      return { databases };
    } catch (error) {
      throw new Error(`Failed to list databases: ${error}`);
    }
  }

  async getDatabase(resourceGroupName: string, serverName: string, databaseName: string) {
    try {
      const database = await this.managementClient.databases.get(resourceGroupName, serverName, databaseName);
      return {
        name: database.name,
        location: database.location,
        status: database.status,
        collation: database.collation,
        maxSizeBytes: database.maxSizeBytes,
        requestedServiceObjectiveName: database.requestedServiceObjectiveName,
        creationDate: database.creationDate,
        earliestRestoreDate: database.earliestRestoreDate,
        tags: database.tags
      };
    } catch (error) {
      throw new Error(`Failed to get database: ${error}`);
    }
  }

  async executeQuery(serverName: string, databaseName: string, username: string, password: string, query: string) {
    return new Promise((resolve, reject) => {
      const config = {
        server: serverName,
        authentication: {
          type: 'default' as const,
          options: {
            userName: username,
            password: password
          }
        },
        options: {
          database: databaseName,
          encrypt: true,
          rowCollectionOnRequestCompletion: true
        }
      };

      const connection = new Connection(config);

      connection.on('connect', (err: any) => {
        if (err) {
          reject(new Error(`Connection failed: ${err.message}`));
          return;
        }

        const request = new Request(query, (err: any, rowCount: any, rows: any) => {
          if (err) {
            reject(new Error(`Query execution failed: ${err.message}`));
            return;
          }

          const results = rows?.map((row: any) => {
            const rowData: Record<string, any> = {};
            row.forEach((column: any) => {
              rowData[column.metadata.colName] = column.value;
            });
            return rowData;
          }) || [];

          resolve({
            rowCount,
            rows: results,
            message: `Query executed successfully. ${rowCount} row(s) affected.`
          });

          connection.close();
        });

        connection.execSql(request);
      });

      connection.on('error', (err: any) => {
        reject(new Error(`Connection error: ${err.message}`));
      });

      connection.connect();
    });
  }

  async listElasticPools(resourceGroupName: string, serverName: string) {
    try {
      const elasticPools = [];
      for await (const pool of this.managementClient.elasticPools.listByServer(resourceGroupName, serverName)) {
        elasticPools.push({
          name: pool.name,
          location: pool.location,
          state: pool.state,
          creationDate: pool.creationDate
        });
      }
      return { elasticPools };
    } catch (error) {
      throw new Error(`Failed to list elastic pools: ${error}`);
    }
  }

  async getElasticPool(resourceGroupName: string, serverName: string, elasticPoolName: string) {
    try {
      const pool = await this.managementClient.elasticPools.get(resourceGroupName, serverName, elasticPoolName);
      return {
        name: pool.name,
        location: pool.location,
        state: pool.state,
        creationDate: pool.creationDate,
        tags: pool.tags
      };
    } catch (error) {
      throw new Error(`Failed to get elastic pool: ${error}`);
    }
  }

  async listFirewallRules(resourceGroupName: string, serverName: string) {
    try {
      const firewallRules = [];
      for await (const rule of this.managementClient.firewallRules.listByServer(resourceGroupName, serverName)) {
        firewallRules.push({
          name: rule.name,
          startIpAddress: rule.startIpAddress,
          endIpAddress: rule.endIpAddress
        });
      }
      return { firewallRules };
    } catch (error) {
      throw new Error(`Failed to list firewall rules: ${error}`);
    }
  }

  async getFirewallRule(resourceGroupName: string, serverName: string, firewallRuleName: string) {
    try {
      const rule = await this.managementClient.firewallRules.get(resourceGroupName, serverName, firewallRuleName);
      return {
        name: rule.name,
        startIpAddress: rule.startIpAddress,
        endIpAddress: rule.endIpAddress
      };
    } catch (error) {
      throw new Error(`Failed to get firewall rule: ${error}`);
    }
  }

  async getDatabaseUsage(resourceGroupName: string, serverName: string, databaseName: string) {
    try {
      const usages = [];
      for await (const usage of this.managementClient.databaseUsages.listByDatabase(resourceGroupName, serverName, databaseName)) {
        usages.push({
          name: usage.name,
          displayName: usage.displayName,
          currentValue: usage.currentValue,
          limit: usage.limit,
          unit: usage.unit,
          nextResetTime: (usage as any).nextResetTime || null
        });
      }
      return { usages };
    } catch (error) {
      throw new Error(`Failed to get database usage: ${error}`);
    }
  }

  async getServerUsage(resourceGroupName: string, serverName: string) {
    try {
      const usages = [];
      for await (const usage of this.managementClient.serverUsages.listByServer(resourceGroupName, serverName)) {
        usages.push({
          name: usage.name,
          displayName: usage.displayName,
          currentValue: usage.currentValue,
          limit: usage.limit,
          unit: usage.unit,
          nextResetTime: (usage as any).nextResetTime || null
        });
      }
      return { usages };
    } catch (error) {
      throw new Error(`Failed to get server usage: ${error}`);
    }
  }
}