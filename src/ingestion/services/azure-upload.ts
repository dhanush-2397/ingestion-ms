import {Injectable} from '@nestjs/common';
import {BlobServiceClient} from "@azure/storage-blob";
import {readFileSync} from "fs";

@Injectable()
export class AzureUpload {

    private blobServiceClient;
    private connectionStr: string;
    private containerName: string;

    constructor() {
        this.connectionStr = process.env.CONNECTION_STRING;
        this.blobServiceClient = BlobServiceClient.fromConnectionString(this.connectionStr);
    }

    public listContainers(): Promise<any> {

        return new Promise(async (resolve, reject) => {
            const containerList: any[] = [];
            for await(const container of this.blobServiceClient.listContainers()) {
                containerList.push(container);
            }
            resolve(containerList);
        });
    }

    public async uploadBlob(container: string, localFileFullPath: string, uploadFilePath: string) {
        // if the file gets too large convert to stream, currently not much info is there
        /*const containerClientStream = this.blobServiceClient.createWriteStreamToBlockBlob(container, uploadFilePath,
            (err, result, res) => {

            }
            const blockBlobClientStream = containerClient.createWriteStreamToBlockBlob();
        );*/
        try{
            const localFile = readFileSync(localFileFullPath);
            const containerClient = this.blobServiceClient.getContainerClient(container);
            const blockClient = containerClient.getBlockBlobClient(uploadFilePath);
            return blockClient.upload(localFile, localFile.length);
        }catch (e) {
            console.error(`azure-upload.uploadBlob: container - ${container} , localFileFullPath - ${localFileFullPath}, uploadFilePath - ${uploadFilePath} `,e);
        }


        /*this.containerClient = container + '/' + path + '/' + fileName;
        this.containerClient = this.blobServiceClient.getContainerClient(container);
        let blockBlobClient = this.containerClient.getBlockBlobClient(fileName);
        return blockBlobClient.upload(content, content.length);*/

    }
}
