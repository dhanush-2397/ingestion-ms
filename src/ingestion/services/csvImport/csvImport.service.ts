import {Injectable} from "@nestjs/common";
import {HttpCustomService} from "../HttpCustomService";
import {Result} from "../../interfaces/Ingestion-data";
import {GenericFunction} from "../generic-function";
import {ReadStream} from "fs";
import {IngestionDatasetQuery} from "../../query/ingestionQuery";
import {DatabaseService} from '../../../database/database.service';

const fs = require('fs');
const {parse} = require('@fast-csv/parse');

let csvImportSchema = {
    "type": "object",
    "properties": {
        "ingestion_type": {
            "type": "string",
            "enum": [
                "event",
                "dataset",
                "dimension"
            ]
        },
        "ingestion_name": {
            "type": "string",
            "shouldnotnull": true
        }
    },
    "required": [
        "ingestion_type",
        "ingestion_name"
    ]
};

interface CSVInputBodyInterface {
    ingestion_type: string;
    ingestion_name: string;
}

interface CSVAPIResponse {
    message: string;
    invalid_record_count: number;
    valid_record_count: number;
}

@Injectable()
export class CsvImportService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService) {
    }

    async readAndParseFile(inputBody: CSVInputBodyInterface, file: Express.Multer.File): Promise<Result> {
        return new Promise(async (resolve, reject) => {
            const isValidSchema: any = await this.service.ajvValidator(csvImportSchema, inputBody);
            if (isValidSchema.errors) {
                reject({code: 400, error: isValidSchema.errors});
            } else {
                const fileCompletePath = file.path;
                const fileSize = file.size;
                const uploadedFileName = file.originalname;

                const queryStr = await IngestionDatasetQuery.createFileTracker(uploadedFileName, inputBody.ingestion_type, inputBody.ingestion_name, fileSize);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult.length === 1) {
                    this.asyncProcessing(inputBody, fileCompletePath, queryResult[0].pid);
                    resolve({code: 200, message: 'File is being processed'})
                } else {
                    resolve({code: 400, error: 'File is not Tracked'})
                }
            }
        });
    }

    async asyncProcessing(inputBody: CSVInputBodyInterface, fileCompletePath: string, fileTrackerPid: number) {
        try {
            const ingestionType = inputBody.ingestion_type, ingestionName = inputBody.ingestion_name;
            const batchLimit: number = 100000;
            let batchCounter: number = 0,
                ingestionTypeBodyArray: any = [], apiResponseDataList: CSVAPIResponse[] = [];
            const csvReadStream = fs.createReadStream(fileCompletePath)
                .pipe(parse({headers: true}))
                .on('data', (csvrow) => {

                    let numberChecking: number;
                    for (let key in csvrow) {
                        numberChecking = Number(csvrow[key]);
                        if (!key.includes('_id') && !key.includes('_year') && !isNaN(numberChecking)) {
                            csvrow[key] = numberChecking;
                        }
                    }
                    batchCounter++;
                    ingestionTypeBodyArray.push({...csvrow});
                    if (batchCounter > batchLimit) {
                        batchCounter = 0;
                        csvReadStream.pause();
                        this.resetAndMakeAPICall(ingestionType, ingestionName, ingestionTypeBodyArray, csvReadStream, apiResponseDataList, false);
                        ingestionTypeBodyArray = []
                    }
                })
                .on('error', async (err) => {
                    console.error('csvImport.service:asyncProcessing:Steam error: ', err);
                    // delete the file
                    fs.unlinkSync(fileCompletePath);
                    const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, 'Error');
                    await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                })
                .on('end', async () => {
                    try {
                        // flush the remaining csv data to API
                        if (ingestionTypeBodyArray.length > 0) {
                            batchCounter = 0;
                            await this.resetAndMakeAPICall(ingestionType, ingestionName, ingestionTypeBodyArray, csvReadStream, apiResponseDataList, true);
                            ingestionTypeBodyArray = undefined;
                            const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, 'Uploaded', ingestionName);
                            await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        }
                        fs.unlinkSync(fileCompletePath);
                    } catch (apiErr) {
                        let validObject = 0, invalidObject = 0;
                        for (let responseData of apiResponseDataList) {
                            invalidObject += responseData.invalid_record_count;
                            validObject += responseData.valid_record_count;
                        }
                        apiResponseDataList = undefined;
                        // delete the file
                        let apiErrorData: any = {};
                        apiErrorData = JSON.parse(apiErr.message);
                        const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, `Error ->${apiErrorData.message}`);
                        await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        fs.unlinkSync(fileCompletePath);
                    }
                    // delete the file
                });
        } catch (e) {
            console.error('csvImport.service.asyncProcessing: ', e.message);
        }
    }

    async resetAndMakeAPICall(ingestionType: string, ingestionName: string, ingestionTypeBodyArray: any[],
                              csvReadStream: ReadStream, apiResponseData: CSVAPIResponse[], isEnd = false) {
        let postBody: any = {};
        const url: string = process.env.URL + `/ingestion/${ingestionType}`;
        const mainKey = ingestionType + '_name';
        postBody[mainKey] = ingestionName;
        if (ingestionType === 'dataset') {
            postBody[ingestionType] = {
                "items": [...ingestionTypeBodyArray]
            }
        } else {
            postBody[ingestionType] = [...ingestionTypeBodyArray];
        }
        try {
            let response = await this.http.post<CSVAPIResponse>(url, postBody);
            apiResponseData.push(response.data);
            if (!isEnd) {
                csvReadStream.resume();
            }
        } catch (apiErr) {
            if (isEnd) {
                throw new Error(JSON.stringify(apiErr.response?.data || apiErr.message))
            } else {
                csvReadStream.destroy(apiErr.response?.data || apiErr.message);
            }
            return;
        }
    }
}