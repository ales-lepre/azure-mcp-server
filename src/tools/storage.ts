import { BlobServiceClient, StorageSharedKeyCredential } from '@azure/storage-blob';
import { getAzureCredential } from '../auth.js';
import { AzureConfig } from '../config.js';

export class StorageTools {
  private client: BlobServiceClient;
  
  constructor(private config: AzureConfig) {
    if (!config.storageAccountName) {
      throw new Error('Storage account name is required');
    }
    
    if (config.storageAccountKey) {
      const credential = new StorageSharedKeyCredential(
        config.storageAccountName,
        config.storageAccountKey
      );
      this.client = new BlobServiceClient(
        `https://${config.storageAccountName}.blob.core.windows.net`,
        credential
      );
    } else {
      const credential = getAzureCredential(config);
      this.client = new BlobServiceClient(
        `https://${config.storageAccountName}.blob.core.windows.net`,
        credential
      );
    }
  }

  async listContainers() {
    try {
      const containers = [];
      for await (const container of this.client.listContainers()) {
        containers.push({
          name: container.name,
          metadata: container.metadata
        });
      }
      return { containers };
    } catch (error) {
      throw new Error(`Failed to list containers: ${error}`);
    }
  }

  async createContainer(name: string, publicAccess: 'blob' | 'container' | null = null) {
    try {
      const containerClient = this.client.getContainerClient(name);
      await containerClient.create({ 
        access: publicAccess === null ? undefined : publicAccess 
      });
      return { message: `Container '${name}' created successfully` };
    } catch (error) {
      throw new Error(`Failed to create container: ${error}`);
    }
  }

  async deleteContainer(name: string) {
    try {
      const containerClient = this.client.getContainerClient(name);
      await containerClient.delete();
      return { message: `Container '${name}' deleted successfully` };
    } catch (error) {
      throw new Error(`Failed to delete container: ${error}`);
    }
  }

  async listBlobs(containerName: string, prefix?: string) {
    try {
      const containerClient = this.client.getContainerClient(containerName);
      const blobs = [];
      
      for await (const blob of containerClient.listBlobsFlat({ prefix })) {
        blobs.push({
          name: blob.name,
          lastModified: blob.properties.lastModified,
          contentLength: blob.properties.contentLength,
          contentType: blob.properties.contentType,
          metadata: blob.metadata
        });
      }
      return { blobs };
    } catch (error) {
      throw new Error(`Failed to list blobs: ${error}`);
    }
  }

  async uploadBlob(containerName: string, blobName: string, content: string, contentType?: string) {
    try {
      const containerClient = this.client.getContainerClient(containerName);
      const blobClient = containerClient.getBlockBlobClient(blobName);
      
      await blobClient.upload(content, Buffer.byteLength(content), {
        blobHTTPHeaders: { blobContentType: contentType }
      });
      
      return { 
        message: `Blob '${blobName}' uploaded successfully`,
        url: blobClient.url
      };
    } catch (error) {
      throw new Error(`Failed to upload blob: ${error}`);
    }
  }

  async downloadBlob(containerName: string, blobName: string) {
    try {
      const containerClient = this.client.getContainerClient(containerName);
      const blobClient = containerClient.getBlobClient(blobName);
      
      const downloadResponse = await blobClient.download();
      const content = await this.streamToBuffer(downloadResponse.readableStreamBody!);
      
      return {
        content: content.toString(),
        contentType: downloadResponse.contentType,
        lastModified: downloadResponse.lastModified,
        size: content.length
      };
    } catch (error) {
      throw new Error(`Failed to download blob: ${error}`);
    }
  }

  async deleteBlob(containerName: string, blobName: string) {
    try {
      const containerClient = this.client.getContainerClient(containerName);
      const blobClient = containerClient.getBlobClient(blobName);
      
      await blobClient.delete();
      return { message: `Blob '${blobName}' deleted successfully` };
    } catch (error) {
      throw new Error(`Failed to delete blob: ${error}`);
    }
  }

  async getBlobProperties(containerName: string, blobName: string) {
    try {
      const containerClient = this.client.getContainerClient(containerName);
      const blobClient = containerClient.getBlobClient(blobName);
      
      const properties = await blobClient.getProperties();
      
      return {
        lastModified: properties.lastModified,
        contentLength: properties.contentLength,
        contentType: properties.contentType,
        etag: properties.etag,
        metadata: properties.metadata
      };
    } catch (error) {
      throw new Error(`Failed to get blob properties: ${error}`);
    }
  }

  async copyBlob(sourceContainer: string, sourceBlobName: string, destContainer: string, destBlobName: string) {
    try {
      const sourceContainerClient = this.client.getContainerClient(sourceContainer);
      const sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobName);
      
      const destContainerClient = this.client.getContainerClient(destContainer);
      const destBlobClient = destContainerClient.getBlobClient(destBlobName);
      
      await destBlobClient.beginCopyFromURL(sourceBlobClient.url);
      return { message: `Blob copied from '${sourceContainer}/${sourceBlobName}' to '${destContainer}/${destBlobName}'` };
    } catch (error) {
      throw new Error(`Failed to copy blob: ${error}`);
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