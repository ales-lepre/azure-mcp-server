import { DataLakeServiceClient } from '@azure/storage-file-datalake';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class DataLakeTools {
  private client: DataLakeServiceClient;
  
  constructor(private config: AzureConfig) {
    if (!config.dataLakeAccountName) {
      throw new Error('Data Lake account name is required');
    }
    
    const credential = getAzureCredential(config);
    this.client = new DataLakeServiceClient(
      `https://${config.dataLakeAccountName}.dfs.core.windows.net`,
      credential
    );
  }

  async listFileSystems() {
    try {
      const fileSystems = [];
      for await (const fileSystem of this.client.listFileSystems()) {
        fileSystems.push({
          name: fileSystem.name,
          metadata: fileSystem.metadata
        });
      }
      return { fileSystems };
    } catch (error) {
      throw new Error(`Failed to list file systems: ${error}`);
    }
  }

  async createFileSystem(name: string) {
    try {
      const fileSystemClient = this.client.getFileSystemClient(name);
      await fileSystemClient.create();
      return { message: `File system '${name}' created successfully` };
    } catch (error) {
      throw new Error(`Failed to create file system: ${error}`);
    }
  }

  async listFiles(fileSystemName: string, path: string = '') {
    try {
      const fileSystemClient = this.client.getFileSystemClient(fileSystemName);
      const files = [];
      
      for await (const item of fileSystemClient.listPaths({ path, recursive: false })) {
        files.push({
          name: item.name,
          isDirectory: item.isDirectory,
          lastModified: item.lastModified,
          contentLength: item.contentLength
        });
      }
      return { files };
    } catch (error) {
      throw new Error(`Failed to list files: ${error}`);
    }
  }

  async readFile(fileSystemName: string, filePath: string) {
    try {
      const fileSystemClient = this.client.getFileSystemClient(fileSystemName);
      const fileClient = fileSystemClient.getFileClient(filePath);
      
      const downloadResponse = await fileClient.read();
      const content = await this.streamToBuffer(downloadResponse.readableStreamBody!);
      
      return {
        content: content.toString(),
        size: content.length,
        lastModified: downloadResponse.lastModified
      };
    } catch (error) {
      throw new Error(`Failed to read file: ${error}`);
    }
  }

  async uploadFile(fileSystemName: string, filePath: string, content: string) {
    try {
      const fileSystemClient = this.client.getFileSystemClient(fileSystemName);
      const fileClient = fileSystemClient.getFileClient(filePath);
      
      await fileClient.create();
      await fileClient.append(Buffer.from(content), 0, content.length);
      await fileClient.flush(content.length);
      
      return { message: `File '${filePath}' uploaded successfully` };
    } catch (error) {
      throw new Error(`Failed to upload file: ${error}`);
    }
  }

  async deleteFile(fileSystemName: string, filePath: string) {
    try {
      const fileSystemClient = this.client.getFileSystemClient(fileSystemName);
      const fileClient = fileSystemClient.getFileClient(filePath);
      
      await fileClient.delete();
      return { message: `File '${filePath}' deleted successfully` };
    } catch (error) {
      throw new Error(`Failed to delete file: ${error}`);
    }
  }

  async createDirectory(fileSystemName: string, directoryPath: string) {
    try {
      const fileSystemClient = this.client.getFileSystemClient(fileSystemName);
      const directoryClient = fileSystemClient.getDirectoryClient(directoryPath);
      
      await directoryClient.create();
      return { message: `Directory '${directoryPath}' created successfully` };
    } catch (error) {
      throw new Error(`Failed to create directory: ${error}`);
    }
  }

  private async streamToBuffer(readableStream: NodeJS.ReadableStream): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      readableStream.on('data', (data) => {
        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
      });
      readableStream.on('end', () => {
        resolve(Buffer.concat(chunks));
      });
      readableStream.on('error', reject);
    });
  }
}