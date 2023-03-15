import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import {Result} from "../../interfaces/Ingestion-data";
import { uploadFilestoS3 } from "../s3-upload";
import { resolve } from "path";

@Injectable()
export class V4DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService) {
    }

    async uploadFiles() {
        let result = await uploadFilestoS3()
        console.log(result);
        return result;
    }
}