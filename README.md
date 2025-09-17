# Azure MCP Server

Een Model Context Protocol (MCP) server voor Microsoft Azure services inclusief Data Lake, Storage Account en Fabric.

## Features

- **Azure Data Lake Storage Gen2**: Beheer file systems, upload/download bestanden, maak directories
- **Azure Blob Storage**: Beheer containers, upload/download blobs, kopieer bestanden  
- **Azure Fabric / Power BI**: Beheer workspaces, datasets, reports, dashboards, lakehouses en notebooks

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
```

## Claude Desktop Configuratie

Voeg de server toe aan je Claude Desktop configuratie (`~/Library/Application Support/Claude/claude_desktop_config.json` op macOS):

```json
{
  "mcpServers": {
    "azure": {
      "command": "node",
      "args": ["/path/to/azure-mcp-server/build/index.js"],
      "env": {
        "AZURE_TENANT_ID": "your-tenant-id",
        "AZURE_CLIENT_ID": "your-client-id",
        "AZURE_CLIENT_SECRET": "your-client-secret", 
        "AZURE_SUBSCRIPTION_ID": "your-subscription-id",
        "AZURE_DATALAKE_ACCOUNT_NAME": "yourdatalakeaccount",
        "AZURE_STORAGE_ACCOUNT_NAME": "yourstorageaccount",
        "AZURE_STORAGE_ACCOUNT_KEY": "your-storage-key",
        "AZURE_FABRIC_WORKSPACE_ID": "your-workspace-id"
      }
    }
  }
}
```

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

## License

MIT