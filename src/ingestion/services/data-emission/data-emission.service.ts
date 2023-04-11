import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import {Result} from "../../interfaces/Ingestion-data";
import {IngestionDatasetQuery} from "../../query/ingestionQuery";
import {UploadService} from "../file-uploader-service";

@Injectable()
export class DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService, private uploadService: UploadService) {
    }

    async readAndParseFile(File: Express.Multer.File): Promise<Result> {
        return new Promise(async (resolve, reject) => {
            const fileSize = File.size;
            const uploadedFileName = File.originalname;

            const queryStr = await IngestionDatasetQuery.createFileTracker(uploadedFileName, 'Emission', uploadedFileName, fileSize);
            const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
            let filePath = `./emission-files/` + uploadedFileName;
            if (queryResult?.length === 1) {
                let fileTrackerPid = queryResult[0].pid;
                let folderName = await this.service.getDate();

                if (process.env.STORAGE_TYPE === 'local') {
                    await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, filePath, `emission/${folderName}/`);
                } else if (process.env.STORAGE_TYPE === 'azure') {
                    await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, filePath, `emission/${folderName}/`);
                } else if (process.env.STORAGE_TYPE === 'oracle') {
                    await this.uploadService.uploadFiles('oracle', `${process.env.ORACLE_BUCKET}`, filePath, `emission/${folderName}/`);
                } else {
                    await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, filePath, `emission/${folderName}/`);
                }

                const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, 'Uploaded', uploadedFileName);
                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                await this.service.deleteLocalFile(filePath);
                resolve({code: 200, message: 'File uploaded successfully'})
            } else {
                resolve({code: 400, error: 'File is not Tracked'})
            }
        });
    }
}