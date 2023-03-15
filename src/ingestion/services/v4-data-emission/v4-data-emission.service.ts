import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import { uploadFilestoS3 } from "../s3-upload";

@Injectable()
export class V4DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService) {
    }

    async uploadFiles() {
        let files = process.env.INPUT_LOCATION;
        try{
            let result = await uploadFilestoS3(process.env.AWS_EMISSION_BUCKET,files)
            console.log('result===>', result);
            return result;
       }catch(error){
        console.error("error is", error.message);
        throw new Error(error)
       }
    }
}