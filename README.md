# Azure MCP Server

Een Model Context Protocol (MCP) server voor Microsoft Azure services met uitgebreide ondersteuning voor cloud data platforms, analytics, en enterprise data warehouse oplossingen.

## Features

### Core Azure Services
- **Azure Data Lake Storage Gen2**: Beheer file systems, upload/download bestanden, maak directories
- **Azure Blob Storage**: Beheer containers, upload/download blobs, kopieer bestanden
- **Microsoft Fabric / Power BI**: Beheer workspaces, datasets, reports, dashboards, lakehouses en notebooks
- **Azure Data Factory**: ETL/ELT pipeline orchestratie en monitoring
- **Azure SQL Database**: Relationele database operaties en query uitvoering
- **Azure Databricks**: Big data processing, cluster en job management
- **Azure Monitor**: Operationele monitoring, alerts en diagnostics
- **Azure Synapse Analytics**: Uitgebreide analytics platform operaties

### Enterprise Data Warehouse Capabilities
- **Dimensional Modeling**: Geautomatiseerde fact en dimension tabel generatie met SCD ondersteuning
- **Star Schema Design**: Complete star schema creatie met validatie en best practices
- **Pipeline Orchestration**: End-to-end ETL pipeline generatie en deployment
- **Data Quality Framework**: Geautomatiseerde data quality regels en monitoring
- **Data Warehouse Architecture**: Multi-layer enterprise DW design (Raw → Staging → Curated → Mart)
- **Data Catalog Management**: Metadata management en lineage tracking

## Installatie

1. Clone de repository en installeer dependencies:
```bash
git clone <repository-url>
cd azure-mcp-server
npm install
```

2. Build het project:
```bash
npm run build
```

## Configuratie

Stel de volgende environment variabelen in:

### Authenticatie (kies een methode):

**Optie 1: Service Principal (aanbevolen)**
```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"  
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_SUBSCRIPTION_ID="your-subscription-id"
```

**Optie 2: Gebruik DefaultAzureCredential**
De server gebruikt automatisch `DefaultAzureCredential` als geen service principal is geconfigureerd.

### Service configuratie:

```bash
# Data Lake Storage
export AZURE_DATALAKE_ACCOUNT_NAME="yourdatalakeaccount"

# Blob Storage
export AZURE_STORAGE_ACCOUNT_NAME="yourstorageaccount"
export AZURE_STORAGE_ACCOUNT_KEY="your-storage-key"  # optioneel, gebruikt anders managed identity

# Fabric/Power BI
export AZURE_FABRIC_WORKSPACE_ID="your-workspace-id"  # optioneel, kan ook per tool call worden opgegeven
export AZURE_FABRIC_TOKEN="your-fabric-token"  # voor geavanceerde pipeline orchestratie
export AZURE_FABRIC_WORKSPACE_URL="https://your-workspace.fabric.microsoft.com"  # optioneel

# Azure Databricks
export AZURE_DATABRICKS_TOKEN="your-databricks-token"  # optioneel
export AZURE_DATABRICKS_WORKSPACE_URL="https://your-workspace.azuredatabricks.net"  # optioneel

# Azure Synapse Analytics
export AZURE_SYNAPSE_TOKEN="your-synapse-token"  # optioneel
export AZURE_SYNAPSE_WORKSPACE_URL="https://your-workspace.dev.azuresynapse.net"  # optioneel

# Azure Monitor
export AZURE_LOG_ANALYTICS_WORKSPACE_ID="your-log-analytics-workspace-id"  # optioneel
```

## Claude Desktop Configuratie

Voeg de server toe aan je Claude Desktop configuratie:

### Configuratie Bestand Locatie
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

### Complete Configuratie (Alle Services)

```json
{
  "mcpServers": {
    "azure-mcp-server": {
      "command": "node",
      "args": ["/path/to/azure-mcp-server/build/index.js"],
      "env": {
        // Vereist: Azure Authenticatie
        "AZURE_TENANT_ID": "your-tenant-id",
        "AZURE_CLIENT_ID": "your-client-id",
        "AZURE_CLIENT_SECRET": "your-client-secret",
        "AZURE_SUBSCRIPTION_ID": "your-subscription-id",

        // Optioneel: Storage Services
        "AZURE_DATALAKE_ACCOUNT_NAME": "your-datalake-account",
        "AZURE_STORAGE_ACCOUNT_NAME": "your-storage-account",
        "AZURE_STORAGE_ACCOUNT_KEY": "your-storage-key",

        // Optioneel: Microsoft Fabric
        "AZURE_FABRIC_WORKSPACE_ID": "your-fabric-workspace-id",
        "AZURE_FABRIC_TOKEN": "your-fabric-token",
        "AZURE_FABRIC_WORKSPACE_URL": "https://your-workspace.fabric.microsoft.com",

        // Optioneel: Azure Databricks
        "AZURE_DATABRICKS_TOKEN": "your-databricks-token",
        "AZURE_DATABRICKS_WORKSPACE_URL": "https://your-workspace.azuredatabricks.net",

        // Optioneel: Azure Synapse Analytics
        "AZURE_SYNAPSE_TOKEN": "your-synapse-token",
        "AZURE_SYNAPSE_WORKSPACE_URL": "https://your-workspace.dev.azuresynapse.net",

        // Optioneel: Azure Monitor
        "AZURE_LOG_ANALYTICS_WORKSPACE_ID": "your-log-analytics-workspace-id"
      }
    }
  }
}
```

### Minimale Configuratie (Alleen Core Services)

Voor basis Azure services (Data Factory, SQL Database, Monitor) is alleen dit nodig:

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

### Service-specifieke Notities

- **Dimensional Modeling & Data Warehouse Tools**: Altijd beschikbaar (geen extra configuratie nodig)
- **Azure Storage & Data Lake**: Vereisen storage account namen en optionele keys
- **Microsoft Fabric Geavanceerde Pipelines**: Vereisen workspace token en URL voor pipeline orchestratie
- **Databricks & Synapse**: Vereisen workspace-specifieke tokens voor cluster/pool management
- **Monitor**: Optionele workspace ID voor log analytics queries

## Beschikbare Tools

### Data Lake Storage

- `datalake_list_filesystems` - Toon alle file systems
- `datalake_create_filesystem` - Maak een nieuw file system
- `datalake_list_files` - Toon bestanden en mappen in een file system
- `datalake_read_file` - Lees een bestand
- `datalake_upload_file` - Upload een bestand
- `datalake_delete_file` - Verwijder een bestand
- `datalake_create_directory` - Maak een directory

### Blob Storage

- `storage_list_containers` - Toon alle containers
- `storage_create_container` - Maak een nieuwe container
- `storage_delete_container` - Verwijder een container
- `storage_list_blobs` - Toon blobs in een container
- `storage_upload_blob` - Upload een blob
- `storage_download_blob` - Download een blob
- `storage_delete_blob` - Verwijder een blob
- `storage_get_blob_properties` - Toon blob eigenschappen
- `storage_copy_blob` - Kopieer een blob

### Fabric / Power BI

- `fabric_list_workspaces` - Toon alle workspaces
- `fabric_get_workspace` - Toon workspace details
- `fabric_list_datasets` - Toon datasets
- `fabric_list_reports` - Toon reports
- `fabric_list_dashboards` - Toon dashboards
- `fabric_list_dataflows` - Toon dataflows
- `fabric_get_dataset` - Toon dataset details
- `fabric_refresh_dataset` - Ververs een dataset
- `fabric_get_dataset_refresh_history` - Toon refresh geschiedenis
- `fabric_execute_dax` - Voer een DAX query uit
- `fabric_list_lakehouses` - Toon lakehouses
- `fabric_get_lakehouse` - Toon lakehouse details
- `fabric_list_notebooks` - Toon notebooks

### Azure Data Factory

- `datafactory_list_factories` - Toon alle data factories
- `datafactory_get_factory` - Toon factory details
- `datafactory_list_pipelines` - Toon pipelines in een factory
- `datafactory_get_pipeline` - Toon pipeline details
- `datafactory_trigger_pipeline` - Start een pipeline
- `datafactory_get_pipeline_runs` - Toon pipeline run geschiedenis
- `datafactory_get_activity_runs` - Toon activity runs voor een pipeline
- `datafactory_list_datasets` - Toon datasets
- `datafactory_get_dataset` - Toon dataset details
- `datafactory_list_linked_services` - Toon linked services
- `datafactory_get_linked_service` - Toon linked service details
- `datafactory_list_triggers` - Toon triggers
- `datafactory_get_trigger` - Toon trigger details

### Azure SQL Database

- `sql_list_servers` - Toon alle SQL servers
- `sql_get_server` - Toon server details
- `sql_list_databases` - Toon databases op een server
- `sql_get_database` - Toon database details
- `sql_execute_query` - Voer een SQL query uit
- `sql_list_elastic_pools` - Toon elastic pools
- `sql_get_elastic_pool` - Toon elastic pool details
- `sql_list_firewall_rules` - Toon firewall regels
- `sql_create_firewall_rule` - Maak een firewall regel
- `sql_delete_firewall_rule` - Verwijder een firewall regel

### Azure Databricks

- `databricks_list_clusters` - Toon alle clusters
- `databricks_get_cluster` - Toon cluster details
- `databricks_start_cluster` - Start een cluster
- `databricks_stop_cluster` - Stop een cluster
- `databricks_create_cluster` - Maak een nieuw cluster
- `databricks_delete_cluster` - Verwijder een cluster
- `databricks_list_jobs` - Toon alle jobs
- `databricks_get_job` - Toon job details
- `databricks_run_job` - Start een job
- `databricks_list_workspaces` - Toon workspace objecten
- `databricks_upload_notebook` - Upload een notebook
- `databricks_download_notebook` - Download een notebook

### Azure Monitor

- `monitor_list_action_groups` - Toon alle action groups
- `monitor_get_action_group` - Toon action group details
- `monitor_create_action_group` - Maak een action group
- `monitor_delete_action_group` - Verwijder een action group
- `monitor_list_metric_alerts` - Toon metric alerts
- `monitor_get_metric_alert` - Toon metric alert details
- `monitor_create_metric_alert` - Maak een metric alert
- `monitor_delete_metric_alert` - Verwijder een metric alert
- `monitor_list_diagnostic_settings` - Toon diagnostic settings
- `monitor_create_diagnostic_setting` - Maak een diagnostic setting
- `monitor_query_logs` - Voer een KQL query uit op logs

### Azure Synapse Analytics

- `synapse_list_workspaces` - Toon alle Synapse workspaces
- `synapse_get_workspace` - Toon workspace details
- `synapse_list_sql_pools` - Toon SQL pools
- `synapse_get_sql_pool` - Toon SQL pool details
- `synapse_pause_sql_pool` - Pauzeer een SQL pool
- `synapse_resume_sql_pool` - Hervat een SQL pool
- `synapse_list_spark_pools` - Toon Spark pools
- `synapse_get_spark_pool` - Toon Spark pool details
- `synapse_list_pipelines` - Toon Synapse pipelines
- `synapse_trigger_pipeline` - Start een Synapse pipeline
- `synapse_list_notebooks` - Toon notebooks
- `synapse_list_data_flows` - Toon data flows

### Dimensional Modeling Tools

- `dimensional_generate_dimension` - Genereer een dimension tabel met SCD ondersteuning
- `dimensional_generate_fact` - Genereer een fact tabel met measures en foreign keys
- `dimensional_generate_star_schema` - Genereer een complete star schema
- `dimensional_validate_star_schema` - Valideer star schema design
- `dimensional_generate_scd_procedures` - Genereer stored procedures voor SCD afhandeling
- `dimensional_create_dimension_indexes` - Maak optimale indexes voor dimensions
- `dimensional_create_fact_indexes` - Maak optimale indexes voor facts

### Fabric Pipeline Orchestration

- `fabric_create_dimension_pipeline` - Maak ETL pipeline voor dimension loading met SCD
- `fabric_create_fact_pipeline` - Maak ETL pipeline voor fact loading
- `fabric_create_star_schema_pipeline` - Maak complete star schema ETL workflow
- `fabric_schedule_pipeline` - Plan pipeline uitvoering met triggers
- `fabric_create_data_quality_pipeline` - Maak data quality monitoring pipeline
- `fabric_create_incremental_pipeline` - Maak incrementele data loading pipeline

### Data Warehouse Management

- `dw_create_architecture` - Maak multi-layer DW architectuur (Raw → Staging → Curated → Mart)
- `dw_generate_standard_dimensions` - Genereer standaard dimensies (Date, Time, Geography, Currency)
- `dw_create_data_quality_framework` - Maak data quality framework met regels
- `dw_create_data_catalog` - Maak data catalog met metadata en lineage
- `dw_generate_data_mart` - Genereer gespecialiseerde data marts
- `dw_create_aggregation_tables` - Maak aggregatie tabellen voor performance
- `dw_generate_olap_views` - Genereer OLAP views voor analytics
- `dw_create_monitoring_dashboard` - Maak monitoring dashboard voor DW health

## Tool Categories

De server biedt **elf categorieën** van tools:

### Core Azure Services (8 categorieën)
1. **Data Lake Tools** (`datalake_*`): File system management, file CRUD operaties
2. **Storage Tools** (`storage_*`): Container management, blob CRUD operaties
3. **Fabric Tools** (`fabric_*`): Workspace, dataset, report, lakehouse management
4. **Data Factory Tools** (`datafactory_*`): Pipeline orchestratie, run monitoring
5. **SQL Database Tools** (`sql_*`): Server en database management, query uitvoering
6. **Databricks Tools** (`databricks_*`): Workspace management, cluster lifecycle
7. **Monitor Tools** (`monitor_*`): Action groups, metric alerts, diagnostics
8. **Synapse Tools** (`synapse_*`): Workspace management, SQL/Spark pools

### Enterprise Data Warehouse Tools (3 categorieën)
9. **Dimensional Modeling Tools** (`dimensional_*`): SCD dimensies, fact tabellen, star schemas
10. **Fabric Pipeline Orchestration** (`fabric_create_*`): Complexe ETL pipelines, SCD handling
11. **Data Warehouse Management** (`dw_*`): Multi-layer DW architectuur, data quality, catalogs

## Development

```bash
# Development mode (compileert en start)
npm run dev

# Build alleen
npm run build

# Start na build
npm start

# Watch mode (automatisch hercompileren)
npm run watch
```

## Troubleshooting

### Authenticatie Issues
1. Controleer of je Azure credentials correct zijn ingesteld
2. Zorg ervoor dat je service principal de juiste permissions heeft op de resources
3. Voor Fabric: zorg ervoor dat je account Power BI Pro/Premium licentie heeft

### Connection Issues  
1. Controleer of je storage account names en workspace IDs correct zijn
2. Test de verbinding met Azure CLI: `az login` en `az account show`

### Permission Issues
Zorg ervoor dat je service principal de volgende rollen heeft:
- **Storage Blob Data Contributor** voor Blob Storage
- **Storage Blob Data Owner** voor Data Lake Storage
- **Power BI workspace member/admin** voor Fabric
- **Data Factory Contributor** voor Azure Data Factory
- **SQL DB Contributor** voor Azure SQL Database
- **Contributor** voor Databricks en Synapse workspaces
- **Monitoring Contributor** voor Azure Monitor

### Azure Credentials Verkrijgen

1. **Service Principal**: Maak een App Registration in Azure AD
2. **Fabric Token**: Genereer personal access token in Fabric workspace settings
3. **Databricks Token**: Genereer personal access token in Databricks workspace
4. **Synapse Token**: Gebruik Azure AD authenticatie of workspace-specifieke tokens

### Beveiligingsaanbevelingen

- Gebruik environment variabelen of Azure Key Vault voor gevoelige credentials
- Roteer tokens regelmatig volgens je organisatie's beveiligingsbeleid
- Ken minimaal vereiste permissions toe aan service principals
- Overweeg het gebruik van managed identities bij gebruik in Azure omgevingen

## License

MIT