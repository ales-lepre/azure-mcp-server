import { MonitorClient } from '@azure/arm-monitor';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class MonitorTools {
  private monitorClient: MonitorClient;

  constructor(private config: AzureConfig) {
    if (!config.subscriptionId) {
      throw new Error('Subscription ID is required for Monitor operations');
    }

    const credential = getAzureCredential(config);
    this.monitorClient = new MonitorClient(credential, config.subscriptionId);
  }

  async listActionGroups(resourceGroupName: string) {
    try {
      const actionGroups = [];
      for await (const group of this.monitorClient.actionGroups.listByResourceGroup(resourceGroupName)) {
        actionGroups.push({
          name: group.name,
          location: group.location,
          enabled: group.enabled,
          groupShortName: group.groupShortName,
          emailReceivers: group.emailReceivers,
          smsReceivers: group.smsReceivers,
          webhookReceivers: group.webhookReceivers,
          armRoleReceivers: group.armRoleReceivers,
          azureAppPushReceivers: group.azureAppPushReceivers,
          tags: group.tags
        });
      }
      return { actionGroups };
    } catch (error) {
      throw new Error(`Failed to list action groups: ${error}`);
    }
  }

  async getActionGroup(resourceGroupName: string, actionGroupName: string) {
    try {
      const group = await this.monitorClient.actionGroups.get(resourceGroupName, actionGroupName);
      return {
        name: group.name,
        location: group.location,
        enabled: group.enabled,
        groupShortName: group.groupShortName,
        emailReceivers: group.emailReceivers,
        smsReceivers: group.smsReceivers,
        webhookReceivers: group.webhookReceivers,
        itsmReceivers: group.itsmReceivers,
        azureAppPushReceivers: group.azureAppPushReceivers,
        automationRunbookReceivers: group.automationRunbookReceivers,
        voiceReceivers: group.voiceReceivers,
        logicAppReceivers: group.logicAppReceivers,
        azureFunctionReceivers: group.azureFunctionReceivers,
        armRoleReceivers: group.armRoleReceivers,
        tags: group.tags
      };
    } catch (error) {
      throw new Error(`Failed to get action group: ${error}`);
    }
  }

  async listAlertRules(resourceGroupName: string) {
    try {
      const alertRules = [];
      for await (const rule of this.monitorClient.alertRules.listByResourceGroup(resourceGroupName)) {
        alertRules.push({
          name: rule.name,
          location: rule.location,
          isEnabled: rule.isEnabled,
          condition: rule.condition,
          actions: rule.actions,
          description: rule.description,
          lastUpdatedTime: rule.lastUpdatedTime,
          tags: rule.tags
        });
      }
      return { alertRules };
    } catch (error) {
      throw new Error(`Failed to list alert rules: ${error}`);
    }
  }

  async getAlertRule(resourceGroupName: string, ruleName: string) {
    try {
      const rule = await this.monitorClient.alertRules.get(resourceGroupName, ruleName);
      return {
        name: rule.name,
        location: rule.location,
        isEnabled: rule.isEnabled,
        condition: rule.condition,
        actions: rule.actions,
        description: rule.description,
        lastUpdatedTime: rule.lastUpdatedTime,
        tags: rule.tags
      };
    } catch (error) {
      throw new Error(`Failed to get alert rule: ${error}`);
    }
  }

  async listMetricAlerts(resourceGroupName: string) {
    try {
      const metricAlerts = [];
      for await (const alert of this.monitorClient.metricAlerts.listByResourceGroup(resourceGroupName)) {
        metricAlerts.push({
          name: alert.name,
          location: alert.location,
          description: alert.description,
          severity: alert.severity,
          enabled: alert.enabled,
          scopes: alert.scopes,
          evaluationFrequency: alert.evaluationFrequency,
          windowSize: alert.windowSize,
          criteria: alert.criteria,
          actions: alert.actions,
          lastUpdatedTime: alert.lastUpdatedTime,
          tags: alert.tags
        });
      }
      return { metricAlerts };
    } catch (error) {
      throw new Error(`Failed to list metric alerts: ${error}`);
    }
  }

  async getMetricAlert(resourceGroupName: string, alertName: string) {
    try {
      const alert = await this.monitorClient.metricAlerts.get(resourceGroupName, alertName);
      return {
        name: alert.name,
        location: alert.location,
        description: alert.description,
        severity: alert.severity,
        enabled: alert.enabled,
        scopes: alert.scopes,
        evaluationFrequency: alert.evaluationFrequency,
        windowSize: alert.windowSize,
        criteria: alert.criteria,
        actions: alert.actions,
        lastUpdatedTime: alert.lastUpdatedTime,
        autoMitigate: alert.autoMitigate,
        tags: alert.tags
      };
    } catch (error) {
      throw new Error(`Failed to get metric alert: ${error}`);
    }
  }

  async listActivityLogAlerts(resourceGroupName: string) {
    try {
      const activityLogAlerts = [];
      for await (const alert of this.monitorClient.activityLogAlerts.listByResourceGroup(resourceGroupName)) {
        activityLogAlerts.push({
          name: alert.name,
          location: alert.location,
          enabled: alert.enabled,
          scopes: alert.scopes,
          condition: alert.condition,
          actions: alert.actions,
          description: alert.description,
          tags: alert.tags
        });
      }
      return { activityLogAlerts };
    } catch (error) {
      throw new Error(`Failed to list activity log alerts: ${error}`);
    }
  }

  async getActivityLogAlert(resourceGroupName: string, alertName: string) {
    try {
      const alert = await this.monitorClient.activityLogAlerts.get(resourceGroupName, alertName);
      return {
        name: alert.name,
        location: alert.location,
        enabled: alert.enabled,
        scopes: alert.scopes,
        condition: alert.condition,
        actions: alert.actions,
        description: alert.description,
        tags: alert.tags
      };
    } catch (error) {
      throw new Error(`Failed to get activity log alert: ${error}`);
    }
  }

  async queryLogs(workspaceId: string, query: string, timespan?: string) {
    try {
      // Note: This would require @azure/monitor-query-logs for full implementation
      // For now, return a placeholder indicating the feature needs additional setup
      return {
        message: "Log querying requires additional Azure Monitor Query setup. Use Azure CLI or Portal for log queries.",
        workspaceId,
        query,
        timespan
      };
    } catch (error) {
      throw new Error(`Failed to query logs: ${error}`);
    }
  }

  async getMetrics(resourceUri: string, metricNames: string[], timespan?: string, interval?: string) {
    try {
      // Note: This would require @azure/monitor-query-metrics for full implementation
      // For now, return a placeholder indicating the feature needs additional setup
      return {
        message: "Metrics querying requires additional Azure Monitor Query setup. Use Azure CLI or Portal for metrics queries.",
        resourceUri,
        metricNames,
        timespan,
        interval
      };
    } catch (error) {
      throw new Error(`Failed to get metrics: ${error}`);
    }
  }

  async listMetricDefinitions(resourceUri: string) {
    try {
      const definitions = [];
      for await (const definition of this.monitorClient.metricDefinitions.list(resourceUri)) {
        definitions.push({
          name: definition.name,
          displayDescription: definition.displayDescription,
          category: definition.category,
          metricClass: definition.metricClass,
          unit: definition.unit,
          primaryAggregationType: definition.primaryAggregationType,
          supportedAggregationTypes: definition.supportedAggregationTypes,
          metricAvailabilities: definition.metricAvailabilities,
          dimensions: definition.dimensions,
          isDimensionRequired: definition.isDimensionRequired,
          resourceId: definition.resourceId,
          namespace: definition.namespace
        });
      }
      return { definitions };
    } catch (error) {
      throw new Error(`Failed to list metric definitions: ${error}`);
    }
  }

  async listDiagnosticSettings(resourceUri: string) {
    try {
      const settings = await this.monitorClient.diagnosticSettings.list(resourceUri);
      return {
        diagnosticSettings: settings.value?.map((setting: any) => ({
          name: setting.name,
          storageAccountId: setting.storageAccountId,
          serviceBusRuleId: setting.serviceBusRuleId,
          eventHubAuthorizationRuleId: setting.eventHubAuthorizationRuleId,
          eventHubName: setting.eventHubName,
          workspaceId: setting.workspaceId,
          logs: setting.logs,
          metrics: setting.metrics,
          logAnalyticsDestinationType: setting.logAnalyticsDestinationType
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to list diagnostic settings: ${error}`);
    }
  }

  async getDiagnosticSetting(resourceUri: string, settingName: string) {
    try {
      const setting = await this.monitorClient.diagnosticSettings.get(resourceUri, settingName);
      return {
        name: setting.name,
        storageAccountId: setting.storageAccountId,
        serviceBusRuleId: setting.serviceBusRuleId,
        eventHubAuthorizationRuleId: setting.eventHubAuthorizationRuleId,
        eventHubName: setting.eventHubName,
        workspaceId: setting.workspaceId,
        logs: setting.logs,
        metrics: setting.metrics,
        logAnalyticsDestinationType: setting.logAnalyticsDestinationType
      };
    } catch (error) {
      throw new Error(`Failed to get diagnostic setting: ${error}`);
    }
  }

  async listLogProfiles() {
    try {
      const profiles = [];
      for await (const profile of this.monitorClient.logProfiles.list()) {
        profiles.push({
          name: profile.name,
          location: profile.location,
          locations: profile.locations,
          categories: profile.categories,
          retentionPolicy: profile.retentionPolicy,
          serviceBusRuleId: profile.serviceBusRuleId,
          storageAccountId: profile.storageAccountId,
          tags: profile.tags
        });
      }
      return { logProfiles: profiles };
    } catch (error) {
      throw new Error(`Failed to list log profiles: ${error}`);
    }
  }

  async getLogProfile(logProfileName: string) {
    try {
      const profile = await this.monitorClient.logProfiles.get(logProfileName);
      return {
        name: profile.name,
        location: profile.location,
        locations: profile.locations,
        categories: profile.categories,
        retentionPolicy: profile.retentionPolicy,
        serviceBusRuleId: profile.serviceBusRuleId,
        storageAccountId: profile.storageAccountId,
        tags: profile.tags
      };
    } catch (error) {
      throw new Error(`Failed to get log profile: ${error}`);
    }
  }
}