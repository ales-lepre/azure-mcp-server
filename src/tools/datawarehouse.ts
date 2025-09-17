import { AzureConfig } from '../config.js';
import { DimensionTable, FactTable, StarSchema } from './dimensional.js';

export interface DataWarehouseLayer {
  name: string;
  description: string;
  schemas: string[];
  purpose: 'Raw' | 'Staging' | 'Cleansed' | 'Curated' | 'Mart';
}

export interface DataWarehouseSchema {
  name: string;
  description: string;
  layer: DataWarehouseLayer;
  tables: string[];
  views: string[];
  procedures: string[];
}

export interface ColumnLineage {
  sourceColumn: string;
  sourceTable: string;
  targetColumn: string;
  targetTable: string;
  transformation?: string;
  businessRule?: string;
}

export interface DataQualityRule {
  name: string;
  description: string;
  ruleType: 'NotNull' | 'Unique' | 'Range' | 'Format' | 'Lookup' | 'Custom';
  column: string;
  table: string;
  parameters?: Record<string, any>;
  severity: 'Error' | 'Warning' | 'Info';
}

export interface DataCatalogEntry {
  name: string;
  type: 'Table' | 'View' | 'Procedure' | 'Function';
  schema: string;
  description: string;
  businessOwner: string;
  technicalOwner: string;
  classification: 'Public' | 'Internal' | 'Confidential' | 'Restricted';
  tags: string[];
  lastUpdated: string;
  columns?: {
    name: string;
    dataType: string;
    description: string;
    businessName: string;
    isPII: boolean;
    classification: string;
  }[];
}

export class DataWarehouseManagement {
  constructor(private config: AzureConfig) {}

  async createDataWarehouseArchitecture(
    dwName: string,
    layers: DataWarehouseLayer[]
  ) {
    try {
      let sql = `-- Data Warehouse Architecture: ${dwName}\n`;
      sql += `-- Created on: ${new Date().toISOString()}\n\n`;

      // Create database if not exists
      sql += `IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '${dwName}')\n`;
      sql += `BEGIN\n`;
      sql += `    CREATE DATABASE [${dwName}]\n`;
      sql += `END\nGO\n\n`;

      sql += `USE [${dwName}]\nGO\n\n`;

      // Create schemas for each layer
      layers.forEach(layer => {
        sql += `-- ${layer.purpose} Layer: ${layer.name}\n`;
        sql += `-- ${layer.description}\n`;

        layer.schemas.forEach(schema => {
          sql += `IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '${schema}')\n`;
          sql += `    EXEC('CREATE SCHEMA [${schema}]')\nGO\n\n`;
        });
      });

      // Create standard utility objects
      sql += this.generateUtilityObjects();

      // Create audit framework
      sql += this.generateAuditFramework();

      // Create data lineage tracking
      sql += this.generateLineageTracking();

      return {
        dwName,
        layers,
        sqlScript: sql,
        message: `Data warehouse architecture for ${dwName} generated successfully`
      };
    } catch (error) {
      throw new Error(`Failed to create data warehouse architecture: ${error}`);
    }
  }

  async generateStandardDimensions() {
    try {
      const standardDimensions = [
        this.generateDateDimension(),
        this.generateTimeDimension(),
        this.generateGeographyDimension(),
        this.generateCurrencyDimension()
      ];

      let sql = `-- Standard Dimensions for Data Warehouse\n\n`;
      standardDimensions.forEach(dim => {
        sql += dim;
        sql += '\n\n';
      });

      return {
        dimensions: ['DimDate', 'DimTime', 'DimGeography', 'DimCurrency'],
        sqlScript: sql,
        message: 'Standard dimensions generated successfully'
      };
    } catch (error) {
      throw new Error(`Failed to generate standard dimensions: ${error}`);
    }
  }

  private generateDateDimension(): string {
    let sql = `-- Date Dimension\n`;
    sql += `CREATE TABLE [dbo].[DimDate] (\n`;
    sql += `    DateKey INT PRIMARY KEY,\n`;
    sql += `    Date DATE NOT NULL,\n`;
    sql += `    DayOfWeek INT NOT NULL,\n`;
    sql += `    DayName NVARCHAR(10) NOT NULL,\n`;
    sql += `    DayOfMonth INT NOT NULL,\n`;
    sql += `    DayOfYear INT NOT NULL,\n`;
    sql += `    WeekOfYear INT NOT NULL,\n`;
    sql += `    MonthNumber INT NOT NULL,\n`;
    sql += `    MonthName NVARCHAR(10) NOT NULL,\n`;
    sql += `    MonthYear NVARCHAR(7) NOT NULL,\n`;
    sql += `    Quarter INT NOT NULL,\n`;
    sql += `    QuarterName NVARCHAR(2) NOT NULL,\n`;
    sql += `    Year INT NOT NULL,\n`;
    sql += `    IsWeekend BIT NOT NULL,\n`;
    sql += `    IsHoliday BIT NOT NULL DEFAULT 0,\n`;
    sql += `    FiscalYear INT NOT NULL,\n`;
    sql += `    FiscalQuarter INT NOT NULL,\n`;
    sql += `    FiscalMonth INT NOT NULL\n`;
    sql += `);\n\n`;

    // Add date population procedure
    sql += `-- Procedure to populate date dimension\n`;
    sql += `CREATE PROCEDURE [dbo].[PopulateDimDate]\n`;
    sql += `    @StartDate DATE,\n`;
    sql += `    @EndDate DATE\n`;
    sql += `AS\nBEGIN\n`;
    sql += `    DECLARE @CurrentDate DATE = @StartDate;\n`;
    sql += `    \n`;
    sql += `    WHILE @CurrentDate <= @EndDate\n`;
    sql += `    BEGIN\n`;
    sql += `        INSERT INTO [dbo].[DimDate] (\n`;
    sql += `            DateKey, Date, DayOfWeek, DayName, DayOfMonth, DayOfYear,\n`;
    sql += `            WeekOfYear, MonthNumber, MonthName, MonthYear,\n`;
    sql += `            Quarter, QuarterName, Year, IsWeekend,\n`;
    sql += `            FiscalYear, FiscalQuarter, FiscalMonth\n`;
    sql += `        )\n`;
    sql += `        VALUES (\n`;
    sql += `            CAST(FORMAT(@CurrentDate, 'yyyyMMdd') AS INT),\n`;
    sql += `            @CurrentDate,\n`;
    sql += `            DATEPART(WEEKDAY, @CurrentDate),\n`;
    sql += `            DATENAME(WEEKDAY, @CurrentDate),\n`;
    sql += `            DAY(@CurrentDate),\n`;
    sql += `            DATEPART(DAYOFYEAR, @CurrentDate),\n`;
    sql += `            DATEPART(WEEK, @CurrentDate),\n`;
    sql += `            MONTH(@CurrentDate),\n`;
    sql += `            DATENAME(MONTH, @CurrentDate),\n`;
    sql += `            FORMAT(@CurrentDate, 'yyyy-MM'),\n`;
    sql += `            DATEPART(QUARTER, @CurrentDate),\n`;
    sql += `            'Q' + CAST(DATEPART(QUARTER, @CurrentDate) AS VARCHAR(1)),\n`;
    sql += `            YEAR(@CurrentDate),\n`;
    sql += `            CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END,\n`;
    sql += `            YEAR(@CurrentDate),\n`;
    sql += `            DATEPART(QUARTER, @CurrentDate),\n`;
    sql += `            MONTH(@CurrentDate)\n`;
    sql += `        );\n`;
    sql += `        \n`;
    sql += `        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);\n`;
    sql += `    END\n`;
    sql += `END;\n`;

    return sql;
  }

  private generateTimeDimension(): string {
    let sql = `-- Time Dimension\n`;
    sql += `CREATE TABLE [dbo].[DimTime] (\n`;
    sql += `    TimeKey INT PRIMARY KEY,\n`;
    sql += `    Time TIME NOT NULL,\n`;
    sql += `    Hour INT NOT NULL,\n`;
    sql += `    Minute INT NOT NULL,\n`;
    sql += `    Second INT NOT NULL,\n`;
    sql += `    HourName NVARCHAR(8) NOT NULL,\n`;
    sql += `    MinuteName NVARCHAR(8) NOT NULL,\n`;
    sql += `    AMPMIndicator NVARCHAR(2) NOT NULL,\n`;
    sql += `    BusinessHour BIT NOT NULL,\n`;
    sql += `    TimeOfDayDescription NVARCHAR(20) NOT NULL\n`;
    sql += `);\n`;

    return sql;
  }

  private generateGeographyDimension(): string {
    let sql = `-- Geography Dimension\n`;
    sql += `CREATE TABLE [dbo].[DimGeography] (\n`;
    sql += `    GeographyKey BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
    sql += `    GeographyAlternateKey NVARCHAR(50) NOT NULL,\n`;
    sql += `    City NVARCHAR(100),\n`;
    sql += `    StateProvince NVARCHAR(100),\n`;
    sql += `    Country NVARCHAR(100) NOT NULL,\n`;
    sql += `    CountryCode NVARCHAR(3),\n`;
    sql += `    PostalCode NVARCHAR(20),\n`;
    sql += `    Continent NVARCHAR(50),\n`;
    sql += `    Region NVARCHAR(50),\n`;
    sql += `    Latitude DECIMAL(10,8),\n`;
    sql += `    Longitude DECIMAL(11,8),\n`;
    sql += `    TimeZone NVARCHAR(50),\n`;
    sql += `    EffectiveDate DATE NOT NULL DEFAULT GETDATE(),\n`;
    sql += `    ExpiryDate DATE,\n`;
    sql += `    IsCurrent BIT NOT NULL DEFAULT 1\n`;
    sql += `);\n`;

    return sql;
  }

  private generateCurrencyDimension(): string {
    let sql = `-- Currency Dimension\n`;
    sql += `CREATE TABLE [dbo].[DimCurrency] (\n`;
    sql += `    CurrencyKey BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
    sql += `    CurrencyCode NVARCHAR(3) NOT NULL,\n`;
    sql += `    CurrencyName NVARCHAR(100) NOT NULL,\n`;
    sql += `    CurrencySymbol NVARCHAR(5),\n`;
    sql += `    DecimalPlaces INT NOT NULL DEFAULT 2,\n`;
    sql += `    IsActive BIT NOT NULL DEFAULT 1,\n`;
    sql += `    EffectiveDate DATE NOT NULL DEFAULT GETDATE(),\n`;
    sql += `    ExpiryDate DATE,\n`;
    sql += `    IsCurrent BIT NOT NULL DEFAULT 1\n`;
    sql += `);\n`;

    return sql;
  }

  private generateUtilityObjects(): string {
    let sql = `-- Utility Objects\n\n`;

    // Error logging table
    sql += `CREATE TABLE [dbo].[ErrorLog] (\n`;
    sql += `    ErrorLogID BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
    sql += `    ErrorTime DATETIME2 NOT NULL DEFAULT GETDATE(),\n`;
    sql += `    UserName NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,\n`;
    sql += `    ErrorNumber INT,\n`;
    sql += `    ErrorSeverity INT,\n`;
    sql += `    ErrorState INT,\n`;
    sql += `    ErrorProcedure NVARCHAR(256),\n`;
    sql += `    ErrorLine INT,\n`;
    sql += `    ErrorMessage NVARCHAR(4000),\n`;
    sql += `    ETLBatchID BIGINT\n`;
    sql += `);\n\n`;

    // ETL batch control table
    sql += `CREATE TABLE [dbo].[ETLBatchControl] (\n`;
    sql += `    BatchID BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
    sql += `    BatchName NVARCHAR(100) NOT NULL,\n`;
    sql += `    StartTime DATETIME2 NOT NULL DEFAULT GETDATE(),\n`;
    sql += `    EndTime DATETIME2,\n`;
    sql += `    Status NVARCHAR(20) NOT NULL DEFAULT 'Running',\n`;
    sql += `    RecordsProcessed BIGINT DEFAULT 0,\n`;
    sql += `    RecordsInserted BIGINT DEFAULT 0,\n`;
    sql += `    RecordsUpdated BIGINT DEFAULT 0,\n`;
    sql += `    RecordsDeleted BIGINT DEFAULT 0,\n`;
    sql += `    ErrorMessage NVARCHAR(4000)\n`;
    sql += `);\n\n`;

    return sql;
  }

  private generateAuditFramework(): string {
    let sql = `-- Audit Framework\n\n`;

    sql += `CREATE TABLE [dbo].[DataLineage] (\n`;
    sql += `    LineageID BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
    sql += `    SourceSystem NVARCHAR(100) NOT NULL,\n`;
    sql += `    SourceTable NVARCHAR(256) NOT NULL,\n`;
    sql += `    SourceColumn NVARCHAR(128),\n`;
    sql += `    TargetSystem NVARCHAR(100) NOT NULL,\n`;
    sql += `    TargetTable NVARCHAR(256) NOT NULL,\n`;
    sql += `    TargetColumn NVARCHAR(128),\n`;
    sql += `    TransformationRule NVARCHAR(MAX),\n`;
    sql += `    BusinessRule NVARCHAR(MAX),\n`;
    sql += `    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),\n`;
    sql += `    CreatedBy NVARCHAR(100) NOT NULL DEFAULT SYSTEM_USER\n`;
    sql += `);\n\n`;

    return sql;
  }

  private generateLineageTracking(): string {
    let sql = `-- Data Lineage Tracking Procedures\n\n`;

    sql += `CREATE PROCEDURE [dbo].[TrackDataLineage]\n`;
    sql += `    @SourceSystem NVARCHAR(100),\n`;
    sql += `    @SourceTable NVARCHAR(256),\n`;
    sql += `    @SourceColumn NVARCHAR(128) = NULL,\n`;
    sql += `    @TargetSystem NVARCHAR(100),\n`;
    sql += `    @TargetTable NVARCHAR(256),\n`;
    sql += `    @TargetColumn NVARCHAR(128) = NULL,\n`;
    sql += `    @TransformationRule NVARCHAR(MAX) = NULL,\n`;
    sql += `    @BusinessRule NVARCHAR(MAX) = NULL\n`;
    sql += `AS\nBEGIN\n`;
    sql += `    INSERT INTO [dbo].[DataLineage] (\n`;
    sql += `        SourceSystem, SourceTable, SourceColumn,\n`;
    sql += `        TargetSystem, TargetTable, TargetColumn,\n`;
    sql += `        TransformationRule, BusinessRule\n`;
    sql += `    )\n`;
    sql += `    VALUES (\n`;
    sql += `        @SourceSystem, @SourceTable, @SourceColumn,\n`;
    sql += `        @TargetSystem, @TargetTable, @TargetColumn,\n`;
    sql += `        @TransformationRule, @BusinessRule\n`;
    sql += `    );\n`;
    sql += `END;\n\n`;

    return sql;
  }

  async createDataQualityFramework(rules: DataQualityRule[]) {
    try {
      let sql = `-- Data Quality Framework\n\n`;

      // Create data quality results table
      sql += `CREATE TABLE [dbo].[DataQualityResults] (\n`;
      sql += `    ResultID BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
      sql += `    RuleName NVARCHAR(100) NOT NULL,\n`;
      sql += `    TableName NVARCHAR(256) NOT NULL,\n`;
      sql += `    ColumnName NVARCHAR(128),\n`;
      sql += `    CheckDate DATETIME2 NOT NULL DEFAULT GETDATE(),\n`;
      sql += `    Severity NVARCHAR(20) NOT NULL,\n`;
      sql += `    Status NVARCHAR(20) NOT NULL, -- Pass/Fail\n`;
      sql += `    RecordsChecked BIGINT,\n`;
      sql += `    RecordsFailed BIGINT,\n`;
      sql += `    FailurePercentage DECIMAL(5,2),\n`;
      sql += `    ErrorMessage NVARCHAR(MAX),\n`;
      sql += `    ETLBatchID BIGINT\n`;
      sql += `);\n\n`;

      // Generate procedures for each rule type
      rules.forEach(rule => {
        sql += this.generateDataQualityRule(rule);
        sql += '\n\n';
      });

      return {
        qualityFramework: sql,
        rules: rules.map(r => r.name),
        message: 'Data quality framework generated successfully'
      };
    } catch (error) {
      throw new Error(`Failed to create data quality framework: ${error}`);
    }
  }

  private generateDataQualityRule(rule: DataQualityRule): string {
    let sql = `-- Data Quality Rule: ${rule.name}\n`;
    sql += `-- ${rule.description}\n`;
    sql += `CREATE PROCEDURE [dbo].[DQ_${rule.name}]\n`;
    sql += `    @ETLBatchID BIGINT = NULL\n`;
    sql += `AS\nBEGIN\n`;

    switch (rule.ruleType) {
      case 'NotNull':
        sql += `    DECLARE @FailCount BIGINT;\n`;
        sql += `    DECLARE @TotalCount BIGINT;\n\n`;
        sql += `    SELECT @TotalCount = COUNT(*) FROM [${rule.table}];\n`;
        sql += `    SELECT @FailCount = COUNT(*) FROM [${rule.table}] WHERE [${rule.column}] IS NULL;\n\n`;
        break;

      case 'Unique':
        sql += `    DECLARE @FailCount BIGINT;\n`;
        sql += `    DECLARE @TotalCount BIGINT;\n\n`;
        sql += `    SELECT @TotalCount = COUNT(*) FROM [${rule.table}];\n`;
        sql += `    SELECT @FailCount = COUNT(*) - COUNT(DISTINCT [${rule.column}]) FROM [${rule.table}];\n\n`;
        break;

      case 'Range':
        const minValue = rule.parameters?.minValue || 0;
        const maxValue = rule.parameters?.maxValue || 999999;
        sql += `    DECLARE @FailCount BIGINT;\n`;
        sql += `    DECLARE @TotalCount BIGINT;\n\n`;
        sql += `    SELECT @TotalCount = COUNT(*) FROM [${rule.table}];\n`;
        sql += `    SELECT @FailCount = COUNT(*) FROM [${rule.table}] WHERE [${rule.column}] NOT BETWEEN ${minValue} AND ${maxValue};\n\n`;
        break;

      default:
        sql += `    -- Custom rule implementation needed\n`;
        sql += `    DECLARE @FailCount BIGINT = 0;\n`;
        sql += `    DECLARE @TotalCount BIGINT = 0;\n\n`;
        break;
    }

    sql += `    INSERT INTO [dbo].[DataQualityResults] (\n`;
    sql += `        RuleName, TableName, ColumnName, Severity,\n`;
    sql += `        Status, RecordsChecked, RecordsFailed,\n`;
    sql += `        FailurePercentage, ETLBatchID\n`;
    sql += `    )\n`;
    sql += `    VALUES (\n`;
    sql += `        '${rule.name}', '${rule.table}', '${rule.column}', '${rule.severity}',\n`;
    sql += `        CASE WHEN @FailCount = 0 THEN 'Pass' ELSE 'Fail' END,\n`;
    sql += `        @TotalCount, @FailCount,\n`;
    sql += `        CASE WHEN @TotalCount > 0 THEN (@FailCount * 100.0 / @TotalCount) ELSE 0 END,\n`;
    sql += `        @ETLBatchID\n`;
    sql += `    );\n`;
    sql += `END;\n`;

    return sql;
  }

  async generateDataCatalog(entries: DataCatalogEntry[]) {
    try {
      let sql = `-- Data Catalog Tables\n\n`;

      // Create catalog tables
      sql += `CREATE TABLE [dbo].[DataCatalog] (\n`;
      sql += `    CatalogID BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
      sql += `    ObjectName NVARCHAR(256) NOT NULL,\n`;
      sql += `    ObjectType NVARCHAR(50) NOT NULL,\n`;
      sql += `    SchemaName NVARCHAR(128) NOT NULL,\n`;
      sql += `    Description NVARCHAR(MAX),\n`;
      sql += `    BusinessOwner NVARCHAR(100),\n`;
      sql += `    TechnicalOwner NVARCHAR(100),\n`;
      sql += `    Classification NVARCHAR(50),\n`;
      sql += `    Tags NVARCHAR(500),\n`;
      sql += `    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),\n`;
      sql += `    LastUpdated DATETIME2 NOT NULL DEFAULT GETDATE()\n`;
      sql += `);\n\n`;

      sql += `CREATE TABLE [dbo].[DataCatalogColumns] (\n`;
      sql += `    ColumnID BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
      sql += `    CatalogID BIGINT NOT NULL,\n`;
      sql += `    ColumnName NVARCHAR(128) NOT NULL,\n`;
      sql += `    DataType NVARCHAR(100) NOT NULL,\n`;
      sql += `    Description NVARCHAR(MAX),\n`;
      sql += `    BusinessName NVARCHAR(256),\n`;
      sql += `    IsPII BIT NOT NULL DEFAULT 0,\n`;
      sql += `    Classification NVARCHAR(50),\n`;
      sql += `    FOREIGN KEY (CatalogID) REFERENCES [dbo].[DataCatalog](CatalogID)\n`;
      sql += `);\n\n`;

      // Insert catalog entries
      entries.forEach(entry => {
        sql += `-- Catalog entry for ${entry.name}\n`;
        sql += `INSERT INTO [dbo].[DataCatalog] (\n`;
        sql += `    ObjectName, ObjectType, SchemaName, Description,\n`;
        sql += `    BusinessOwner, TechnicalOwner, Classification, Tags\n`;
        sql += `)\nVALUES (\n`;
        sql += `    '${entry.name}', '${entry.type}', '${entry.schema}', '${entry.description}',\n`;
        sql += `    '${entry.businessOwner}', '${entry.technicalOwner}', '${entry.classification}', '${entry.tags.join(', ')}'\n`;
        sql += `);\n\n`;

        if (entry.columns) {
          entry.columns.forEach(column => {
            sql += `INSERT INTO [dbo].[DataCatalogColumns] (\n`;
            sql += `    CatalogID, ColumnName, DataType, Description,\n`;
            sql += `    BusinessName, IsPII, Classification\n`;
            sql += `)\nVALUES (\n`;
            sql += `    SCOPE_IDENTITY(), '${column.name}', '${column.dataType}', '${column.description}',\n`;
            sql += `    '${column.businessName}', ${column.isPII ? 1 : 0}, '${column.classification}'\n`;
            sql += `);\n\n`;
          });
        }
      });

      return {
        catalogEntries: entries.length,
        sqlScript: sql,
        message: 'Data catalog generated successfully'
      };
    } catch (error) {
      throw new Error(`Failed to generate data catalog: ${error}`);
    }
  }

  async generateDataMarts(
    starSchemas: StarSchema[],
    targetDatabase: string = 'DataMart'
  ) {
    try {
      let sql = `-- Data Marts Generation\n`;
      sql += `-- Target Database: ${targetDatabase}\n\n`;

      sql += `USE [${targetDatabase}]\nGO\n\n`;

      starSchemas.forEach(schema => {
        // Create schema-specific mart
        sql += `-- Data Mart for ${schema.name}\n`;
        sql += `CREATE SCHEMA [${schema.name}]\nGO\n\n`;

        // Create aggregate tables
        sql += this.generateAggregateTable(schema);
        sql += '\n\n';

        // Create OLAP views
        sql += this.generateOLAPViews(schema);
        sql += '\n\n';
      });

      return {
        dataMarts: starSchemas.map(s => s.name),
        targetDatabase,
        sqlScript: sql,
        message: `Data marts generated for ${starSchemas.length} star schemas`
      };
    } catch (error) {
      throw new Error(`Failed to generate data marts: ${error}`);
    }
  }

  private generateAggregateTable(schema: StarSchema): string {
    let sql = `-- Aggregate table for ${schema.name}\n`;
    sql += `CREATE TABLE [${schema.name}].[Fact_${schema.name}_Monthly] (\n`;
    sql += `    AggregateKey BIGINT IDENTITY(1,1) PRIMARY KEY,\n`;
    sql += `    YearMonth INT NOT NULL,\n`;

    // Add dimension keys (excluding date)
    schema.factTable.dimensionKeys
      .filter(key => key !== 'DateKey')
      .forEach(key => {
        sql += `    ${key} BIGINT NOT NULL,\n`;
      });

    // Add aggregated measures
    schema.factTable.measures.forEach(measure => {
      sql += `    ${measure}_Sum DECIMAL(18,2),\n`;
      sql += `    ${measure}_Avg DECIMAL(18,2),\n`;
      sql += `    ${measure}_Count BIGINT,\n`;
    });

    sql += `    RecordCount BIGINT NOT NULL,\n`;
    sql += `    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()\n`;
    sql += `);\n`;

    return sql;
  }

  private generateOLAPViews(schema: StarSchema): string {
    let sql = `-- OLAP views for ${schema.name}\n`;

    // Time-based analysis view
    sql += `CREATE VIEW [${schema.name}].[v_TimeAnalysis] AS\n`;
    sql += `SELECT \n`;
    sql += `    d.Year,\n`;
    sql += `    d.Quarter,\n`;
    sql += `    d.MonthName,\n`;

    schema.factTable.measures.forEach(measure => {
      sql += `    SUM(f.${measure}) as Total_${measure},\n`;
      sql += `    AVG(f.${measure}) as Avg_${measure},\n`;
    });

    sql += `    COUNT(*) as RecordCount\n`;
    sql += `FROM [${schema.factTable.name}] f\n`;
    sql += `JOIN [DimDate] d ON f.DateKey = d.DateKey\n`;
    sql += `GROUP BY d.Year, d.Quarter, d.MonthName;\n\n`;

    return sql;
  }
}

