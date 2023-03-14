import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import {Result} from "../../interfaces/Ingestion-data";

@Injectable()
export class V4DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService) {
    }

    async uploadFiles(File: Express.Multer.File): Promise<Result> {
        return new Promise(async (resolve, reject) => {

        });
    }
}