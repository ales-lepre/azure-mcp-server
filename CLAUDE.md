# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Azure MCP (Model Context Protocol) server that provides Claude with tools to interact with Microsoft Azure services. The server acts as a bridge between Claude and Azure APIs, enabling comprehensive cloud operations across data, analytics, and infrastructure services.

### Supported Azure Services

- **Data Lake Storage Gen2**: File system operations, data management
- **Blob Storage**: Container and blob operations
- **Microsoft Fabric (Power BI)**: Workspace, dataset, and report management
- **Azure Data Factory**: ETL/ELT pipeline orchestration and monitoring
- **Azure SQL Database**: Relational database operations and query execution
- **Azure Databricks**: Big data processing, cluster and job management
- **Azure Monitor**: Operational monitoring, alerts, and diagnostics
- **Azure Synapse Analytics**: Comprehensive analytics platform operations

### Enterprise Data Warehouse Capabilities

- **Dimensional Modeling**: Automated fact and dimension table generation with SCD support
- **Star Schema Design**: Complete star schema creation with validation and best practices
- **Pipeline Orchestration**: End-to-end ETL pipeline generation and deployment
- **Data Quality Framework**: Automated data quality rules and monitoring
- **Data Warehouse Architecture**: Multi-layer enterprise DW design (Raw → Staging → Curated → Mart)
- **Data Catalog Management**: Metadata management and lineage tracking

## Development Commands

```bash
# Build the project
npm run build

# Run in development mode (builds and starts)
npm run dev

# Start the built server
npm start

# Watch mode for continuous compilation
npm run watch
```

## Architecture

The codebase follows a modular architecture with clear separation of concerns:

### Core Structure
- `src/index.ts`: Main MCP server entry point that registers and handles all tool calls
- `src/config.ts`: Configuration management using Zod for environment variable validation
- `src/auth.ts`: Azure authentication abstraction (supports both Service Principal and DefaultAzureCredential)

### Tool Modules
- `src/tools/datalake.ts`: Azure Data Lake Storage Gen2 operations (DataLakeTools class)
- `src/tools/storage.ts`: Azure Blob Storage operations (StorageTools class)
- `src/tools/fabric.ts`: Microsoft Fabric/Power BI operations via REST API (FabricTools class)
- `src/tools/datafactory.ts`: Azure Data Factory pipeline management (DataFactoryTools class)
- `src/tools/sqldatabase.ts`: Azure SQL Database operations (SqlDatabaseTools class)
- `src/tools/databricks.ts`: Azure Databricks workspace and cluster management (DatabricksTools class)
- `src/tools/monitor.ts`: Azure Monitor alerts and diagnostics (MonitorTools class)
- `src/tools/synapse.ts`: Azure Synapse Analytics operations (SynapseTools class)
- `src/tools/dimensional.ts`: Dimensional modeling for facts, dimensions, and star schemas (DimensionalModelingTools class)
- `src/tools/fabricpipelines.ts`: Advanced Fabric pipeline orchestration and ETL automation (FabricPipelineOrchestrator class)
- `src/tools/datawarehouse.ts`: Enterprise data warehouse architecture and management (DataWarehouseManagement class)

### Key Architectural Patterns
1. **Conditional Tool Loading**: Tools are only initialized if required environment variables are present
2. **Unified Error Handling**: All tools use consistent error wrapping and JSON response formatting
3. **Authentication Flexibility**: Supports both Service Principal (client_id/secret) and managed identity authentication
4. **MCP Protocol Compliance**: Full implementation of MCP server specification with proper request/response schemas

## Configuration Requirements

The server requires specific environment variables depending on which Azure services you want to use:

### Authentication (choose one):
- **Service Principal**: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_SUBSCRIPTION_ID`
- **Or rely on DefaultAzureCredential** for managed identity/Azure CLI auth

### Service-specific Configuration:
- **Data Lake**: `AZURE_DATALAKE_ACCOUNT_NAME`
- **Blob Storage**: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY` (optional)
- **Fabric**: `AZURE_FABRIC_WORKSPACE_ID` (optional, can be provided per-call)
- **Fabric Pipelines**: `AZURE_FABRIC_TOKEN`, `AZURE_FABRIC_WORKSPACE_URL` (optional, for pipeline orchestration)
- **Databricks**: `AZURE_DATABRICKS_TOKEN`, `AZURE_DATABRICKS_WORKSPACE_URL` (optional)
- **Synapse**: `AZURE_SYNAPSE_TOKEN`, `AZURE_SYNAPSE_WORKSPACE_URL` (optional)
- **Monitor**: `AZURE_LOG_ANALYTICS_WORKSPACE_ID` (optional, for log querying)

### Notes:
- Data Factory, SQL Database, and Monitor tools require only the base authentication (subscription ID)
- Databricks and Synapse require additional workspace-specific tokens for API operations
- All configurations are optional - tools are conditionally loaded based on available environment variables

## Tool Categories

The server exposes eleven categories of tools:

### Core Azure Services
1. **Data Lake Tools** (`datalake_*`): File system management, file CRUD operations, directory creation
2. **Storage Tools** (`storage_*`): Container management, blob CRUD operations, blob copying
3. **Fabric Tools** (`fabric_*`): Workspace, dataset, report, lakehouse, and notebook management via Power BI REST API
4. **Data Factory Tools** (`datafactory_*`): Pipeline orchestration, run monitoring, trigger management, dataset operations
5. **SQL Database Tools** (`sql_*`): Server and database management, query execution, elastic pools, firewall rules
6. **Databricks Tools** (`databricks_*`): Workspace management, cluster lifecycle, job execution, notebook operations
7. **Monitor Tools** (`monitor_*`): Action groups, metric alerts, diagnostic settings, log profiles
8. **Synapse Tools** (`synapse_*`): Workspace management, SQL/Spark pools, pipeline orchestration, data flows

### Enterprise Data Warehouse Tools
9. **Dimensional Modeling Tools** (`dimensional_*`):
   - Generate SCD Type 1/2/3 dimension tables with audit columns
   - Create fact tables with measures and foreign keys
   - Build complete star schemas with validation
   - Generate SQL scripts for tables, indexes, and stored procedures

10. **Fabric Pipeline Orchestration** (`fabric_create_*`):
    - Create complex ETL pipelines with multiple activities
    - Build dimension load pipelines with SCD handling
    - Generate fact load pipelines with dimension lookups
    - Orchestrate complete star schema ETL workflows
    - Schedule pipelines with triggers and monitoring

11. **Data Warehouse Management** (`dw_*`):
    - Create multi-layer DW architecture (Raw → Staging → Curated → Mart)
    - Generate standard dimensions (Date, Time, Geography, Currency)
    - Build data quality frameworks with automated rule checking
    - Create data catalogs with metadata and lineage tracking
    - Generate data marts with aggregation and OLAP views

## Development Notes

- Uses TypeScript with ES modules (`"type": "module"`)
- Builds to `build/` directory
- Uses Zod for runtime configuration validation
- Azure SDK clients are initialized per-tool-class with appropriate credentials
- Fabric, Databricks, and Synapse tools use REST APIs for workspace-specific operations
- SQL Database tools include direct query execution via Tedious library
- Monitor tools provide infrastructure monitoring and alerting capabilities
- All async operations have proper error handling and user-friendly error messages
- Tools are conditionally loaded based on available configuration to minimize resource usage
- Dimensional modeling and data warehouse tools are always available (no external dependencies)
- Advanced pipeline orchestration requires Fabric workspace access tokens

## Claude Desktop Configuration

To use this MCP server with Claude Desktop, add the following configuration to your `claude_desktop_config.json` file:

### Configuration File Location
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

### Complete Configuration Example

```json
{
  "mcpServers": {
    "azure-mcp-server": {
      "command": "node",
      "args": ["/path/to/azure-mcp-server/build/index.js"],
      "env": {
        // Required: Azure Authentication
        "AZURE_TENANT_ID": "your-tenant-id",
        "AZURE_CLIENT_ID": "your-client-id",
        "AZURE_CLIENT_SECRET": "your-client-secret",
        "AZURE_SUBSCRIPTION_ID": "your-subscription-id",

        // Optional: Storage Services
        "AZURE_DATALAKE_ACCOUNT_NAME": "your-datalake-account",
        "AZURE_STORAGE_ACCOUNT_NAME": "your-storage-account",
        "AZURE_STORAGE_ACCOUNT_KEY": "your-storage-key",

        // Optional: Microsoft Fabric
        "AZURE_FABRIC_WORKSPACE_ID": "your-fabric-workspace-id",
        "AZURE_FABRIC_TOKEN": "your-fabric-token",
        "AZURE_FABRIC_WORKSPACE_URL": "https://your-workspace.fabric.microsoft.com",

        // Optional: Azure Databricks
        "AZURE_DATABRICKS_TOKEN": "your-databricks-token",
        "AZURE_DATABRICKS_WORKSPACE_URL": "https://your-workspace.azuredatabricks.net",

        // Optional: Azure Synapse Analytics
        "AZURE_SYNAPSE_TOKEN": "your-synapse-token",
        "AZURE_SYNAPSE_WORKSPACE_URL": "https://your-workspace.dev.azuresynapse.net",

        // Optional: Azure Monitor
        "AZURE_LOG_ANALYTICS_WORKSPACE_ID": "your-log-analytics-workspace-id"
      }
    }
  }
}
```

### Minimal Configuration (Core Services Only)

For basic Azure services (Data Factory, SQL Database, Monitor), you only need:

```json
{
  "mcpServers": {
    "azure-mcp-server": {
      "command": "node",
      "args": ["/path/to/azure-mcp-server/build/index.js"],
      "env": {
        "AZURE_TENANT_ID": "your-tenant-id",
        "AZURE_CLIENT_ID": "your-client-id",
        "AZURE_CLIENT_SECRET": "your-client-secret",
        "AZURE_SUBSCRIPTION_ID": "your-subscription-id"
      }
    }
  }
}
```

### Service-Specific Notes

- **Dimensional Modeling & Data Warehouse Tools**: Always available (no additional config needed)
- **Azure Storage & Data Lake**: Require storage account names and optional keys
- **Microsoft Fabric Advanced Pipelines**: Require workspace token and URL for pipeline orchestration
- **Databricks & Synapse**: Require workspace-specific tokens for cluster/pool management
- **Monitor**: Optional workspace ID for log analytics queries

### Getting Azure Credentials

1. **Service Principal**: Create an App Registration in Azure AD
2. **Fabric Token**: Generate personal access token in Fabric workspace settings
3. **Databricks Token**: Generate personal access token in Databricks workspace
4. **Synapse Token**: Use Azure AD authentication or workspace-specific tokens

### Security Recommendations

- Use environment variables or Azure Key Vault for sensitive credentials
- Rotate tokens regularly according to your organization's security policies
- Grant minimal required permissions to service principals
- Consider using managed identities when running in Azure environments