import { AzureConfig } from '../config.js';

export interface DimensionColumn {
  name: string;
  dataType: string;
  isPrimaryKey?: boolean;
  isBusinessKey?: boolean;
  isSCD?: boolean;
  scdType?: 1 | 2 | 3;
  description?: string;
}

export interface FactColumn {
  name: string;
  dataType: string;
  aggregationType?: 'SUM' | 'COUNT' | 'AVG' | 'MIN' | 'MAX';
  isAdditive?: boolean;
  isMeasure?: boolean;
  description?: string;
}

export interface DimensionTable {
  name: string;
  businessName: string;
  description?: string;
  columns: DimensionColumn[];
  scdType: 1 | 2 | 3;
  naturalKey: string[];
  surrogateKey: string;
}

export interface FactTable {
  name: string;
  businessName: string;
  description?: string;
  columns: FactColumn[];
  measures: string[];
  dimensionKeys: string[];
  grainDescription: string;
}

export interface StarSchema {
  name: string;
  description?: string;
  factTable: FactTable;
  dimensionTables: DimensionTable[];
}

export class DimensionalModelingTools {
  constructor(private config: AzureConfig) {}

  async generateDimensionTable(
    tableName: string,
    businessName: string,
    columns: DimensionColumn[],
    options: {
      scdType?: 1 | 2 | 3;
      naturalKey?: string[];
      surrogateKey?: string;
      description?: string;
    } = {}
  ) {
    try {
      const {
        scdType = 2,
        naturalKey = [],
        surrogateKey = `${tableName}_SK`,
        description = ''
      } = options;

      // Add standard dimension columns
      const standardColumns: DimensionColumn[] = [
        {
          name: surrogateKey,
          dataType: 'BIGINT IDENTITY(1,1)',
          isPrimaryKey: true,
          description: 'Surrogate key for the dimension'
        }
      ];

      // Add SCD Type 2 columns if needed
      if (scdType === 2) {
        standardColumns.push(
          {
            name: 'EffectiveDate',
            dataType: 'DATE',
            description: 'Date when this record became effective'
          },
          {
            name: 'ExpiryDate',
            dataType: 'DATE',
            description: 'Date when this record expires'
          },
          {
            name: 'IsCurrent',
            dataType: 'BIT',
            description: 'Flag indicating if this is the current record'
          },
          {
            name: 'RecordVersion',
            dataType: 'INT',
            description: 'Version number of this record'
          }
        );
      }

      // Add audit columns
      standardColumns.push(
        {
          name: 'CreatedDate',
          dataType: 'DATETIME2',
          description: 'Date when record was created'
        },
        {
          name: 'ModifiedDate',
          dataType: 'DATETIME2',
          description: 'Date when record was last modified'
        },
        {
          name: 'CreatedBy',
          dataType: 'NVARCHAR(100)',
          description: 'User who created the record'
        },
        {
          name: 'ModifiedBy',
          dataType: 'NVARCHAR(100)',
          description: 'User who last modified the record'
        }
      );

      const dimensionTable: DimensionTable = {
        name: tableName,
        businessName,
        description,
        columns: [...standardColumns, ...columns],
        scdType,
        naturalKey,
        surrogateKey
      };

      const sqlScript = this.generateDimensionSQL(dimensionTable);

      return {
        dimensionTable,
        sqlScript,
        message: `Dimension table ${tableName} generated successfully`
      };
    } catch (error) {
      throw new Error(`Failed to generate dimension table: ${error}`);
    }
  }

  async generateFactTable(
    tableName: string,
    businessName: string,
    measures: FactColumn[],
    dimensionKeys: string[],
    options: {
      grainDescription?: string;
      description?: string;
      partitionColumn?: string;
    } = {}
  ) {
    try {
      const {
        grainDescription = 'One row per transaction',
        description = '',
        partitionColumn = 'DateKey'
      } = options;

      // Add standard fact table columns
      const standardColumns: FactColumn[] = [
        {
          name: 'FactKey',
          dataType: 'BIGINT IDENTITY(1,1)',
          description: 'Surrogate key for the fact table'
        }
      ];

      // Add dimension foreign keys
      dimensionKeys.forEach(dimKey => {
        standardColumns.push({
          name: dimKey,
          dataType: 'BIGINT',
          description: `Foreign key to ${dimKey.replace('Key', '')} dimension`
        });
      });

      // Add date key if not already present
      if (!dimensionKeys.includes('DateKey')) {
        standardColumns.push({
          name: 'DateKey',
          dataType: 'INT',
          description: 'Foreign key to Date dimension (YYYYMMDD format)'
        });
      }

      // Add audit columns
      standardColumns.push(
        {
          name: 'CreatedDate',
          dataType: 'DATETIME2',
          description: 'Date when record was created'
        },
        {
          name: 'ETLBatchId',
          dataType: 'BIGINT',
          description: 'ETL batch identifier'
        }
      );

      const factTable: FactTable = {
        name: tableName,
        businessName,
        description,
        columns: [...standardColumns, ...measures],
        measures: measures.filter(m => m.isMeasure).map(m => m.name),
        dimensionKeys: dimensionKeys,
        grainDescription
      };

      const sqlScript = this.generateFactSQL(factTable, partitionColumn);

      return {
        factTable,
        sqlScript,
        message: `Fact table ${tableName} generated successfully`
      };
    } catch (error) {
      throw new Error(`Failed to generate fact table: ${error}`);
    }
  }

  async generateStarSchema(
    schemaName: string,
    factTable: FactTable,
    dimensionTables: DimensionTable[],
    options: {
      description?: string;
      generateViews?: boolean;
    } = {}
  ) {
    try {
      const { description = '', generateViews = true } = options;

      const starSchema: StarSchema = {
        name: schemaName,
        description,
        factTable,
        dimensionTables
      };

      // Generate complete schema SQL
      let schemaSQL = `-- Star Schema: ${schemaName}\n`;
      schemaSQL += `-- ${description}\n\n`;

      // Create schema
      schemaSQL += `CREATE SCHEMA [${schemaName}];\nGO\n\n`;

      // Generate dimension tables
      dimensionTables.forEach(dim => {
        schemaSQL += this.generateDimensionSQL(dim, schemaName);
        schemaSQL += '\n\n';
      });

      // Generate fact table
      schemaSQL += this.generateFactSQL(factTable, 'DateKey', schemaName);

      // Generate views if requested
      if (generateViews) {
        schemaSQL += '\n\n-- Views\n';
        schemaSQL += this.generateStarSchemaView(starSchema, schemaName);
      }

      // Generate stored procedures for SCD operations
      dimensionTables.forEach(dim => {
        if (dim.scdType === 2) {
          schemaSQL += '\n\n';
          schemaSQL += this.generateSCDStoredProcedure(dim, schemaName);
        }
      });

      return {
        starSchema,
        sqlScript: schemaSQL,
        message: `Star schema ${schemaName} generated successfully`
      };
    } catch (error) {
      throw new Error(`Failed to generate star schema: ${error}`);
    }
  }

  private generateDimensionSQL(dimension: DimensionTable, schemaName: string = 'dbo'): string {
    let sql = `-- Dimension Table: ${dimension.name}\n`;
    sql += `-- ${dimension.description}\n`;
    sql += `CREATE TABLE [${schemaName}].[${dimension.name}] (\n`;

    const columnDefinitions = dimension.columns.map(col => {
      let def = `    [${col.name}] ${col.dataType}`;
      if (col.isPrimaryKey) def += ' PRIMARY KEY';
      if (col.description) def += ` -- ${col.description}`;
      return def;
    });

    sql += columnDefinitions.join(',\n');
    sql += '\n);\n';

    // Add indexes
    if (dimension.naturalKey.length > 0) {
      sql += `\nCREATE UNIQUE INDEX IX_${dimension.name}_NK ON [${schemaName}].[${dimension.name}] (${dimension.naturalKey.join(', ')});\n`;
    }

    if (dimension.scdType === 2) {
      sql += `CREATE INDEX IX_${dimension.name}_Current ON [${schemaName}].[${dimension.name}] (IsCurrent) WHERE IsCurrent = 1;\n`;
    }

    return sql;
  }

  private generateFactSQL(fact: FactTable, partitionColumn: string = 'DateKey', schemaName: string = 'dbo'): string {
    let sql = `-- Fact Table: ${fact.name}\n`;
    sql += `-- ${fact.description}\n`;
    sql += `-- Grain: ${fact.grainDescription}\n`;
    sql += `CREATE TABLE [${schemaName}].[${fact.name}] (\n`;

    const columnDefinitions = fact.columns.map(col => {
      let def = `    [${col.name}] ${col.dataType}`;
      if (col.description) def += ` -- ${col.description}`;
      return def;
    });

    sql += columnDefinitions.join(',\n');
    sql += '\n) ON [PRIMARY];\n';

    // Add indexes for dimension keys
    fact.dimensionKeys.forEach(dimKey => {
      sql += `\nCREATE INDEX IX_${fact.name}_${dimKey} ON [${schemaName}].[${fact.name}] ([${dimKey}]);\n`;
    });

    // Add columnstore index for analytics
    sql += `\nCREATE COLUMNSTORE INDEX CCI_${fact.name} ON [${schemaName}].[${fact.name}];\n`;

    return sql;
  }

  private generateStarSchemaView(starSchema: StarSchema, schemaName: string): string {
    let sql = `-- Star Schema View: ${starSchema.name}\n`;
    sql += `CREATE VIEW [${schemaName}].[v_${starSchema.name}] AS\n`;
    sql += `SELECT \n`;

    // Add fact measures
    const factMeasures = starSchema.factTable.measures.map(measure =>
      `    f.[${measure}]`
    );

    // Add dimension attributes (simplified - you'd want to select specific business attributes)
    const dimensionAttributes = starSchema.dimensionTables.map(dim =>
      `    ${dim.name.toLowerCase().substring(0, 3)}.*`
    );

    sql += [...factMeasures, ...dimensionAttributes].join(',\n');
    sql += `\nFROM [${schemaName}].[${starSchema.factTable.name}] f\n`;

    // Add joins to dimensions
    starSchema.dimensionTables.forEach(dim => {
      const alias = dim.name.toLowerCase().substring(0, 3);
      const joinKey = `${dim.name}Key`;
      sql += `LEFT JOIN [${schemaName}].[${dim.name}] ${alias} ON f.[${joinKey}] = ${alias}.[${dim.surrogateKey}]\n`;
      if (dim.scdType === 2) {
        sql += `    AND ${alias}.[IsCurrent] = 1\n`;
      }
    });

    sql += ';\n';

    return sql;
  }

  private generateSCDStoredProcedure(dimension: DimensionTable, schemaName: string): string {
    let sql = `-- SCD Type 2 Stored Procedure for ${dimension.name}\n`;
    sql += `CREATE PROCEDURE [${schemaName}].[usp_Load_${dimension.name}]\n`;
    sql += `    @SourceData NVARCHAR(MAX) -- JSON array of source records\n`;
    sql += `AS\nBEGIN\n`;
    sql += `    SET NOCOUNT ON;\n\n`;

    sql += `    -- Parse JSON source data\n`;
    sql += `    WITH SourceData AS (\n`;
    sql += `        SELECT *\n`;
    sql += `        FROM OPENJSON(@SourceData)\n`;
    sql += `        WITH (\n`;

    // Add JSON parsing for each business column
    const businessColumns = dimension.columns.filter(col =>
      !['EffectiveDate', 'ExpiryDate', 'IsCurrent', 'RecordVersion', 'CreatedDate', 'ModifiedDate', 'CreatedBy', 'ModifiedBy'].includes(col.name) &&
      !col.isPrimaryKey
    );

    const jsonColumns = businessColumns.map(col =>
      `            [${col.name}] ${col.dataType.split('(')[0]} '$."${col.name}"'`
    );

    sql += jsonColumns.join(',\n');
    sql += `\n        )\n    )\n\n`;

    sql += `    -- Implement SCD Type 2 logic here\n`;
    sql += `    -- 1. Identify changed records\n`;
    sql += `    -- 2. Expire old records\n`;
    sql += `    -- 3. Insert new records\n\n`;
    sql += `END;\n`;

    return sql;
  }

  async validateStarSchema(starSchema: StarSchema) {
    try {
      const validationResults = {
        isValid: true,
        warnings: [] as string[],
        errors: [] as string[]
      };

      // Validate fact table has measures
      if (starSchema.factTable.measures.length === 0) {
        validationResults.warnings.push('Fact table has no measures defined');
      }

      // Validate dimension relationships
      starSchema.factTable.dimensionKeys.forEach(dimKey => {
        const dimensionName = dimKey.replace('Key', '');
        const relatedDimension = starSchema.dimensionTables.find(dim =>
          dim.name.toLowerCase() === dimensionName.toLowerCase()
        );

        if (!relatedDimension) {
          validationResults.errors.push(`Fact table references dimension ${dimensionName} which is not defined`);
          validationResults.isValid = false;
        }
      });

      // Validate dimension natural keys
      starSchema.dimensionTables.forEach(dim => {
        if (dim.naturalKey.length === 0) {
          validationResults.warnings.push(`Dimension ${dim.name} has no natural key defined`);
        }

        if (dim.scdType === 2) {
          const hasRequiredSCDColumns = ['EffectiveDate', 'ExpiryDate', 'IsCurrent'].every(col =>
            dim.columns.some(dimCol => dimCol.name === col)
          );

          if (!hasRequiredSCDColumns) {
            validationResults.errors.push(`SCD Type 2 dimension ${dim.name} is missing required SCD columns`);
            validationResults.isValid = false;
          }
        }
      });

      return {
        validationResults,
        message: validationResults.isValid ? 'Star schema validation passed' : 'Star schema validation failed'
      };
    } catch (error) {
      throw new Error(`Failed to validate star schema: ${error}`);
    }
  }

  async generateETLMapping(
    sourceTable: string,
    targetTable: string,
    columnMappings: { source: string; target: string; transformation?: string }[]
  ) {
    try {
      let sql = `-- ETL Mapping: ${sourceTable} -> ${targetTable}\n`;
      sql += `INSERT INTO [${targetTable}] (\n`;

      const targetColumns = columnMappings.map(mapping => `    [${mapping.target}]`);
      sql += targetColumns.join(',\n');
      sql += '\n)\nSELECT\n';

      const sourceExpressions = columnMappings.map(mapping => {
        let expr = mapping.transformation || `[${mapping.source}]`;
        return `    ${expr} AS [${mapping.target}]`;
      });

      sql += sourceExpressions.join(',\n');
      sql += `\nFROM [${sourceTable}];\n`;

      return {
        etlScript: sql,
        mappings: columnMappings,
        message: `ETL mapping generated for ${sourceTable} -> ${targetTable}`
      };
    } catch (error) {
      throw new Error(`Failed to generate ETL mapping: ${error}`);
    }
  }
}