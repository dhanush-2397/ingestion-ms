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
                this.asyncProcessing(inputBody, fileCompletePath);
                resolve({code: 200, message: 'File is being processed'})

            }
        });
    }

    async asyncProcessing(inputBody: CSVInputBodyInterface, fileCompletePath: string) {
        return new Promise(async (resolve, reject) => {
            // will not implement reject since it will be error and crash the server
            try {
                const ingestionType = inputBody.ingestion_type, ingestionName = inputBody.ingestion_name;
                const batchLimit: number = 100000;
                let batchCounter: number = 0,
                    ingestionTypeBodyArray: any = [], apiResponseDataList: CSVAPIResponse[] = [];
                let queryStr, queryResult, schema;
                switch (ingestionType) {
                    case 'event':
                        queryStr = await IngestionDatasetQuery.getEvents(ingestionName);
                        queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        if (queryResult?.length === 1) {
                            schema = queryResult[0].schema;
                        }
                        break;
                    case 'dimension':
                        queryStr = await IngestionDatasetQuery.getDimension(ingestionName);
                        queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        if (queryResult?.length === 1) {
                            schema = queryResult[0].schema;
                        }
                        break;
                    default:
                        queryStr = await IngestionDatasetQuery.getDataset(ingestionName);
                        queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        if (queryResult?.length === 1) {
                            schema = queryResult[0].schema;
                        }
                }

                const csvReadStream = fs.createReadStream(fileCompletePath)
                    .pipe(parse({headers: true}))
                    .on('data', (csvrow) => {
                        this.service.formatDataToCSVBySchema(csvrow, schema, false);
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
                        // delete the file
                        try {
                            await fs.unlinkSync(fileCompletePath);
                            resolve(`Error -> file stream error ${err}`);
                        } catch (e) {
                            console.error('csvImport.service.file delete error: ', e);
                        }
                    })
                    .on('end', async () => {
                        try {
                            // flush the remaining csv data to API
                            // console.log('csvImport.service.ingestionTypeBodyArray: ', ingestionTypeBodyArray);
                            if (ingestionTypeBodyArray.length > 0) {
                                batchCounter = 0;
                                await this.resetAndMakeAPICall(ingestionType, ingestionName, ingestionTypeBodyArray, csvReadStream, apiResponseDataList, true);
                                ingestionTypeBodyArray = undefined;
                            }
                            // delete the file
                            try {
                                await fs.unlinkSync(fileCompletePath);
                            } catch (e) {
                                console.error('csvImport.service.file delete error: ', e);
                            }

                            resolve(`Success -> complete`);
                        } catch (apiErr) {
                            /*let validObject = 0, invalidObject = 0;
                            console.log('csvImport.service.apiErr: ', apiResponseDataList);
                            for (let responseData of apiResponseDataList) {
                                invalidObject += responseData.invalid_record_count;
                                validObject += responseData.valid_record_count;
                            }*/
                            apiResponseDataList = undefined;
                            let apiErrorData: any = {};
                            apiErrorData = JSON.parse(apiErr.message);
                            // delete the file
                            fs.unlinkSync(fileCompletePath);
                            resolve(`Error END -> API err ${apiErr.message}`);
                        }
                    });
            } catch (e) {
                console.error('csvImport.service.asyncProcessing: ', e.message);
                resolve('Error -> catch error ' + e.message)
            }
        });
    }

    async resetAndMakeAPICall(ingestionType: string, ingestionName: string, ingestionTypeBodyArray: any[],
                              csvReadStream: ReadStream, apiResponseData: CSVAPIResponse[], isEnd = false) {
        let postBody: any = {};
        const url: string = process.env.URL + `/ingestion/${ingestionType}`;
        const mainKey = ingestionType + '_name';
        postBody[mainKey] = ingestionName;

        postBody[ingestionType] = [...ingestionTypeBodyArray];
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