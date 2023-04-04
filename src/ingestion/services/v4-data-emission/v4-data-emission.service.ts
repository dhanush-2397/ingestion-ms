import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import {UploadService} from "../file-uploader-service";

@Injectable()
export class V4DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService, private uploadService: UploadService) {
    }

    async uploadFiles() {
        let inputPath = process.env.INPUT_LOCATION;
        let folderName = await this.service.getDate();
        try {
            if (process.env.STORAGE_TYPE === 'local') {
                await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, inputPath, `emission/${folderName}/`, true);
            } else if (process.env.STORAGE_TYPE === 'azure') {
                await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, inputPath, `emission/${folderName}/`, true);
            } else {
                await this.uploadService.uploadFiles('aws', process.env.AWS_BUCKET, inputPath, `emission/${folderName}/`, true);
            }
            return {
                code: 200,
                message: "Files uploaded successfully"
            }
        } catch (error) {
            console.error("error is", error.message);
            throw new Error(error)
        }
    }
}