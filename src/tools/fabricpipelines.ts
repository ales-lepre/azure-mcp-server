import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export interface PipelineActivity {
  name: string;
  type: 'Copy' | 'DataFlow' | 'Notebook' | 'SparkJob' | 'Lookup' | 'IfCondition' | 'ForEach' | 'Wait' | 'Execute';
  description?: string;
  dependsOn?: string[];
  inputs?: Record<string, any>;
  outputs?: Record<string, any>;
  typeProperties?: Record<string, any>;
}

export interface DataFlowSource {
  name: string;
  dataset: string;
  transformation?: string;
}

export interface DataFlowSink {
  name: string;
  dataset: string;
  transformation?: string;
}

export interface DataFlowDefinition {
  name: string;
  description?: string;
  sources: DataFlowSource[];
  sinks: DataFlowSink[];
  transformations: string[];
}

export interface PipelineDefinition {
  name: string;
  description?: string;
  parameters?: Record<string, any>;
  variables?: Record<string, any>;
  activities: PipelineActivity[];
}

export class FabricPipelineOrchestrator {
  private fabricToken?: string;
  private workspaceUrl?: string;

  constructor(private config: AzureConfig & { fabricToken?: string; fabricWorkspaceUrl?: string }) {
    this.fabricToken = config.fabricToken;
    this.workspaceUrl = config.fabricWorkspaceUrl;
  }

  private async makeApiRequest(endpoint: string, method: string = 'GET', body?: any) {
    if (!this.fabricToken || !this.workspaceUrl) {
      throw new Error('Fabric token and workspace URL are required for pipeline operations');
    }

    const url = `${this.workspaceUrl}/api/v1/${endpoint}`;
    const options: RequestInit = {
      method,
      headers: {
        'Authorization': `Bearer ${this.fabricToken}`,
        'Content-Type': 'application/json'
      }
    };

    if (body && method !== 'GET') {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Fabric API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
  }

  async createDataPipeline(pipelineDefinition: PipelineDefinition) {
    try {
      const pipelineBody = {
        properties: {
          description: pipelineDefinition.description,
          parameters: pipelineDefinition.parameters || {},
          variables: pipelineDefinition.variables || {},
          activities: pipelineDefinition.activities.map(activity => ({
            name: activity.name,
            type: activity.type,
            description: activity.description,
            dependsOn: activity.dependsOn?.map(dep => ({
              activity: dep,
              dependencyConditions: ['Succeeded']
            })) || [],
            typeProperties: activity.typeProperties || {},
            inputs: activity.inputs ? [activity.inputs] : [],
            outputs: activity.outputs ? [activity.outputs] : []
          }))
        }
      };

      const response = await this.makeApiRequest(
        `pipelines/${pipelineDefinition.name}`,
        'PUT',
        pipelineBody
      );

      return {
        pipelineId: response.id,
        name: pipelineDefinition.name,
        message: `Pipeline ${pipelineDefinition.name} created successfully`
      };
    } catch (error) {
      throw new Error(`Failed to create data pipeline: ${error}`);
    }
  }

  async createDataFlow(dataFlowDefinition: DataFlowDefinition) {
    try {
      const dataFlowBody = {
        properties: {
          description: dataFlowDefinition.description,
          sources: dataFlowDefinition.sources.map(source => ({
            name: source.name,
            dataset: {
              referenceName: source.dataset,
              type: 'DatasetReference'
            },
            transformation: source.transformation
          })),
          sinks: dataFlowDefinition.sinks.map(sink => ({
            name: sink.name,
            dataset: {
              referenceName: sink.dataset,
              type: 'DatasetReference'
            },
            transformation: sink.transformation
          })),
          transformations: dataFlowDefinition.transformations.map(transform => ({
            name: `transform_${Date.now()}`,
            script: transform
          }))
        }
      };

      const response = await this.makeApiRequest(
        `dataflows/${dataFlowDefinition.name}`,
        'PUT',
        dataFlowBody
      );

      return {
        dataFlowId: response.id,
        name: dataFlowDefinition.name,
        message: `Data flow ${dataFlowDefinition.name} created successfully`
      };
    } catch (error) {
      throw new Error(`Failed to create data flow: ${error}`);
    }
  }

  async createDimensionLoadPipeline(
    dimensionName: string,
    sourceDataset: string,
    targetDataset: string,
    naturalKeys: string[],
    scdType: 1 | 2 = 2
  ) {
    try {
      const activities: PipelineActivity[] = [];

      // Lookup activity to check for existing records
      activities.push({
        name: 'LookupExistingRecords',
        type: 'Lookup',
        description: `Lookup existing records in ${dimensionName}`,
        typeProperties: {
          source: {
            type: 'AzureSqlSource',
            sqlReaderQuery: `SELECT ${naturalKeys.join(', ')}, ${dimensionName}_SK FROM ${targetDataset} WHERE IsCurrent = 1`
          },
          dataset: {
            referenceName: targetDataset,
            type: 'DatasetReference'
          },
          firstRowOnly: false
        }
      });

      if (scdType === 2) {
        // Data flow for SCD Type 2 processing
        activities.push({
          name: 'ProcessSCDType2',
          type: 'DataFlow',
          description: `Process SCD Type 2 for ${dimensionName}`,
          dependsOn: ['LookupExistingRecords'],
          typeProperties: {
            dataflow: {
              referenceName: `df_${dimensionName}_SCD2`,
              type: 'DataFlowReference'
            },
            compute: {
              coreCount: 8,
              computeType: 'General'
            }
          }
        });
      } else {
        // Simple upsert for SCD Type 1
        activities.push({
          name: 'UpsertDimension',
          type: 'Copy',
          description: `Upsert records in ${dimensionName}`,
          dependsOn: ['LookupExistingRecords'],
          typeProperties: {
            source: {
              type: 'AzureSqlSource'
            },
            sink: {
              type: 'AzureSqlSink',
              upsertSettings: {
                useTempDB: true,
                keys: naturalKeys
              }
            }
          }
        });
      }

      const pipelineDefinition: PipelineDefinition = {
        name: `pl_Load_${dimensionName}`,
        description: `Load ${dimensionName} dimension with SCD Type ${scdType}`,
        parameters: {
          SourceDataset: {
            type: 'string',
            defaultValue: sourceDataset
          },
          TargetDataset: {
            type: 'string',
            defaultValue: targetDataset
          }
        },
        activities
      };

      return await this.createDataPipeline(pipelineDefinition);
    } catch (error) {
      throw new Error(`Failed to create dimension load pipeline: ${error}`);
    }
  }

  async createFactLoadPipeline(
    factName: string,
    sourceDataset: string,
    targetDataset: string,
    dimensionMappings: { source: string; dimension: string; naturalKey: string[] }[]
  ) {
    try {
      const activities: PipelineActivity[] = [];

      // Lookup activities for each dimension
      dimensionMappings.forEach((mapping, index) => {
        activities.push({
          name: `Lookup${mapping.dimension}`,
          type: 'Lookup',
          description: `Lookup ${mapping.dimension} keys`,
          typeProperties: {
            source: {
              type: 'AzureSqlSource',
              sqlReaderQuery: `SELECT ${mapping.naturalKey.join(', ')}, ${mapping.dimension}_SK FROM ${mapping.dimension} WHERE IsCurrent = 1`
            },
            dataset: {
              referenceName: mapping.dimension,
              type: 'DatasetReference'
            },
            firstRowOnly: false
          }
        });
      });

      // Data flow to join source with dimension lookups
      activities.push({
        name: 'TransformFactData',
        type: 'DataFlow',
        description: `Transform and load ${factName} fact table`,
        dependsOn: dimensionMappings.map(mapping => `Lookup${mapping.dimension}`),
        typeProperties: {
          dataflow: {
            referenceName: `df_${factName}_Transform`,
            type: 'DataFlowReference'
          },
          compute: {
            coreCount: 16,
            computeType: 'General'
          }
        }
      });

      // Copy activity to load fact table
      activities.push({
        name: 'LoadFactTable',
        type: 'Copy',
        description: `Load ${factName} fact table`,
        dependsOn: ['TransformFactData'],
        typeProperties: {
          source: {
            type: 'AzureSqlSource'
          },
          sink: {
            type: 'AzureSqlSink',
            writeBehavior: 'insert'
          }
        }
      });

      const pipelineDefinition: PipelineDefinition = {
        name: `pl_Load_${factName}`,
        description: `Load ${factName} fact table with dimension lookups`,
        parameters: {
          SourceDataset: {
            type: 'string',
            defaultValue: sourceDataset
          },
          TargetDataset: {
            type: 'string',
            defaultValue: targetDataset
          },
          LoadDate: {
            type: 'string',
            defaultValue: '@utcnow()'
          }
        },
        activities
      };

      return await this.createDataPipeline(pipelineDefinition);
    } catch (error) {
      throw new Error(`Failed to create fact load pipeline: ${error}`);
    }
  }

  async createMasterPipeline(
    name: string,
    childPipelines: string[],
    options: {
      sequential?: boolean;
      failOnError?: boolean;
      description?: string;
    } = {}
  ) {
    try {
      const { sequential = false, failOnError = true, description = '' } = options;

      const activities: PipelineActivity[] = [];

      if (sequential) {
        // Execute pipelines sequentially
        childPipelines.forEach((pipeline, index) => {
          activities.push({
            name: `Execute_${pipeline}`,
            type: 'Execute',
            description: `Execute ${pipeline}`,
            dependsOn: index > 0 ? [`Execute_${childPipelines[index - 1]}`] : undefined,
            typeProperties: {
              pipeline: {
                referenceName: pipeline,
                type: 'PipelineReference'
              },
              waitOnCompletion: true
            }
          });
        });
      } else {
        // Execute pipelines in parallel
        childPipelines.forEach(pipeline => {
          activities.push({
            name: `Execute_${pipeline}`,
            type: 'Execute',
            description: `Execute ${pipeline}`,
            typeProperties: {
              pipeline: {
                referenceName: pipeline,
                type: 'PipelineReference'
              },
              waitOnCompletion: true
            }
          });
        });
      }

      const pipelineDefinition: PipelineDefinition = {
        name,
        description: description || `Master pipeline orchestrating: ${childPipelines.join(', ')}`,
        parameters: {
          ExecutionDate: {
            type: 'string',
            defaultValue: '@utcnow()'
          }
        },
        activities
      };

      return await this.createDataPipeline(pipelineDefinition);
    } catch (error) {
      throw new Error(`Failed to create master pipeline: ${error}`);
    }
  }

  async createStarSchemaETLPipeline(
    starSchemaName: string,
    dimensionPipelines: string[],
    factPipelines: string[]
  ) {
    try {
      const activities: PipelineActivity[] = [];

      // Execute all dimension pipelines in parallel first
      dimensionPipelines.forEach(pipeline => {
        activities.push({
          name: `Load_${pipeline}`,
          type: 'Execute',
          description: `Load ${pipeline} dimension`,
          typeProperties: {
            pipeline: {
              referenceName: pipeline,
              type: 'PipelineReference'
            },
            waitOnCompletion: true
          }
        });
      });

      // Execute fact pipelines after dimensions are loaded
      factPipelines.forEach(pipeline => {
        activities.push({
          name: `Load_${pipeline}`,
          type: 'Execute',
          description: `Load ${pipeline} fact table`,
          dependsOn: dimensionPipelines.map(dim => `Load_${dim}`),
          typeProperties: {
            pipeline: {
              referenceName: pipeline,
              type: 'PipelineReference'
            },
            waitOnCompletion: true
          }
        });
      });

      // Add data quality checks
      activities.push({
        name: 'DataQualityCheck',
        type: 'Lookup',
        description: 'Perform data quality checks',
        dependsOn: factPipelines.map(fact => `Load_${fact}`),
        typeProperties: {
          source: {
            type: 'AzureSqlSource',
            sqlReaderQuery: `
              SELECT
                '${starSchemaName}' as SchemaName,
                GETDATE() as CheckDate,
                COUNT(*) as TotalFactRecords
              FROM [${factPipelines[0]}]
            `
          }
        }
      });

      const pipelineDefinition: PipelineDefinition = {
        name: `pl_${starSchemaName}_ETL`,
        description: `Complete ETL pipeline for ${starSchemaName} star schema`,
        parameters: {
          LoadDate: {
            type: 'string',
            defaultValue: '@utcnow()'
          },
          FullLoad: {
            type: 'bool',
            defaultValue: false
          }
        },
        activities
      };

      return await this.createDataPipeline(pipelineDefinition);
    } catch (error) {
      throw new Error(`Failed to create star schema ETL pipeline: ${error}`);
    }
  }

  async deployPipelinePackage(
    packageName: string,
    pipelines: PipelineDefinition[],
    dataFlows: DataFlowDefinition[]
  ) {
    try {
      const deploymentResults = {
        packageName,
        deployedPipelines: [] as string[],
        deployedDataFlows: [] as string[],
        errors: [] as string[]
      };

      // Deploy data flows first
      for (const dataFlow of dataFlows) {
        try {
          await this.createDataFlow(dataFlow);
          deploymentResults.deployedDataFlows.push(dataFlow.name);
        } catch (error) {
          deploymentResults.errors.push(`Failed to deploy data flow ${dataFlow.name}: ${error}`);
        }
      }

      // Deploy pipelines
      for (const pipeline of pipelines) {
        try {
          await this.createDataPipeline(pipeline);
          deploymentResults.deployedPipelines.push(pipeline.name);
        } catch (error) {
          deploymentResults.errors.push(`Failed to deploy pipeline ${pipeline.name}: ${error}`);
        }
      }

      return {
        deploymentResults,
        message: `Package ${packageName} deployment completed with ${deploymentResults.errors.length} errors`
      };
    } catch (error) {
      throw new Error(`Failed to deploy pipeline package: ${error}`);
    }
  }

  async monitorPipelineExecution(pipelineName: string, runId: string) {
    try {
      const response = await this.makeApiRequest(`pipelines/${pipelineName}/runs/${runId}`);

      return {
        runId: response.runId,
        pipelineName: response.pipelineName,
        status: response.status,
        startTime: response.runStart,
        endTime: response.runEnd,
        duration: response.durationInMs,
        activities: response.activities?.map((activity: any) => ({
          name: activity.activityName,
          status: activity.status,
          startTime: activity.activityRunStart,
          endTime: activity.activityRunEnd,
          duration: activity.durationInMs
        })) || []
      };
    } catch (error) {
      throw new Error(`Failed to monitor pipeline execution: ${error}`);
    }
  }

  async createScheduledPipeline(
    pipelineName: string,
    schedule: {
      frequency: 'Daily' | 'Weekly' | 'Monthly';
      interval: number;
      startTime: string;
      timeZone?: string;
    }
  ) {
    try {
      const triggerBody = {
        properties: {
          type: 'ScheduleTrigger',
          typeProperties: {
            recurrence: {
              frequency: schedule.frequency,
              interval: schedule.interval,
              startTime: schedule.startTime,
              timeZone: schedule.timeZone || 'UTC'
            }
          },
          pipelines: [
            {
              pipelineReference: {
                referenceName: pipelineName,
                type: 'PipelineReference'
              }
            }
          ]
        }
      };

      const response = await this.makeApiRequest(
        `triggers/tr_${pipelineName}_Schedule`,
        'PUT',
        triggerBody
      );

      // Start the trigger
      await this.makeApiRequest(`triggers/tr_${pipelineName}_Schedule/start`, 'POST');

      return {
        triggerName: `tr_${pipelineName}_Schedule`,
        pipelineName,
        schedule,
        message: `Scheduled trigger created and started for ${pipelineName}`
      };
    } catch (error) {
      throw new Error(`Failed to create scheduled pipeline: ${error}`);
    }
  }
}