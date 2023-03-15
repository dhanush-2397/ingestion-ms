import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import {Result} from "../../interfaces/Ingestion-data";
import {IngestionDatasetQuery} from "../../query/ingestionQuery";
import {uploadToMinio} from "../minio-upload";
import {AzureUpload} from "../azure-upload";

@Injectable()
export class DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService, private azureService: AzureUpload) {
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
                    await uploadToMinio(`${process.env.EMISSION_BUCKET}`, filePath, uploadedFileName, `emission/${folderName}`);
                } else if (process.env.STORAGE_TYPE === 'azure') {
                    await this.azureService.uploadBlob(`${process.env.EMISSION_CONTAINER}`, filePath, `emission/${folderName}/${uploadedFileName}`);
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