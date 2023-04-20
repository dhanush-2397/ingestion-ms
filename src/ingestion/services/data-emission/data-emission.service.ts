import {HttpCustomService} from "../HttpCustomService";
import {Injectable} from "@nestjs/common";
import {DatabaseService} from "../../../database/database.service";
import {GenericFunction} from "../generic-function";
import {Result} from "../../interfaces/Ingestion-data";
import {IngestionDatasetQuery} from "../../query/ingestionQuery";
import {UploadService} from "../file-uploader-service";
import * as AdmZip from 'adm-zip';

const fs = require('fs');
const {parse} = require('@fast-csv/parse');

@Injectable()
export class DataEmissionService {
    constructor(private http: HttpCustomService, private service: GenericFunction, private DatabaseService: DatabaseService, private uploadService: UploadService) {
    }

    async readAndParseFile(File: Express.Multer.File): Promise<Result> {
        return new Promise(async (resolve, reject) => {
            const fileSize = File.size;
            let uploadedFileName = File.originalname;

            const queryStr = await IngestionDatasetQuery.createFileTracker(uploadedFileName, 'Emission', uploadedFileName.split('.zip')[0], fileSize);
            const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);

            //Unzip the file
            uploadedFileName = await this.unzip(File);

            let filePath;
            if (queryResult?.length === 1) {
                let fileTrackerPid = queryResult[0].pid;
                let folderName = await this.service.getDate();
                let validArray = [], records;
                let result = await this.getSchema(uploadedFileName, fileTrackerPid);
                if (result.code === 200) {
                    records = await this.validationFunction(`./temp-files/` + uploadedFileName + ".csv", result.data, uploadedFileName, fileTrackerPid);
                } else {
                    resolve({code: 400, error: `Schema doesn't exists for this program`});
                }

                if (records.validCounter > 0) {
                    filePath = `./emission-files/` + uploadedFileName + ".csv";
                    if (process.env.STORAGE_TYPE === 'local') {
                        await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, filePath, `emission/${folderName}/`);
                    } else if (process.env.STORAGE_TYPE === 'azure') {
                        await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, filePath, `emission/${folderName}/`);
                    } else if (process.env.STORAGE_TYPE === 'oracle') {
                        await this.uploadService.uploadFiles('oracle', `${process.env.ORACLE_BUCKET}`, filePath, `emission/${folderName}/`);
                    } else {
                        await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, filePath, `emission/${folderName}/`);
                    }
                }
                if (records.invalidCounter > 0) {
                    filePath = `./error-files/` + uploadedFileName + "_errors.csv";
                    if (process.env.STORAGE_TYPE === 'local') {
                        await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, filePath, `emission-error/${folderName}/`);
                    } else if (process.env.STORAGE_TYPE === 'azure') {
                        await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, filePath, `emission-error/${folderName}/`);
                    } else if (process.env.STORAGE_TYPE === 'oracle') {
                        await this.uploadService.uploadFiles('oracle', `${process.env.ORACLE_BUCKET}`, filePath, `emission-error/${folderName}/`);
                    } else {
                        await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, filePath, `emission-error/${folderName}/`);
                    }
                }

                const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, 'Uploaded', uploadedFileName);
                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);

                await this.service.deleteLocalFile(`./emission-files/` + uploadedFileName + ".csv");
                await this.service.deleteLocalFile(`./error-files/` + uploadedFileName + "_errors.csv");
                await this.service.deleteLocalFile(`./temp-files/` + uploadedFileName + ".csv");
                await this.service.deleteLocalFile(`./temp-files/` + uploadedFileName + ".zip");
                resolve({code: 200, message: 'File uploaded successfully'})
            } else {
                resolve({code: 400, error: 'File is not Tracked'})
            }
        });
    }

    async unzip(File: Express.Multer.File) {
        const zip = new AdmZip(File.path);
        zip.extractAllTo(`./temp-files/`);
        let fileName = File.originalname.split(".zip")[0];
        return fileName;
    }

    async getSchema(ingestionName, fileTrackerPid) {
        let schema;
        let queryStr = await IngestionDatasetQuery.getEvents(ingestionName);
        let queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
        if (queryResult?.length === 1) {
            schema = queryResult[0].schema;
            return {code: 200, data: schema};
        } else {
            const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, `Error ->No event found`);
            await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
            return {code: 100, message: 'Error ->No event found'};
        }
    }

    async validationFunction(fileCompletePath, schema, ingestionName, fileTrackerPid) {
        return new Promise(async (resolve, reject) => {
            let validArray = [], invalidArray = [];
            let errorCounter = 0, validCounter = 0;
            const batchLimit: number = 100000;
            let batchCounter: number = 0,
                ingestionTypeBodyArray: any = [], apiResponseDataList: any[] = [];

            const csvReadStream = fs.createReadStream(fileCompletePath)
                .pipe(parse({headers: true}))
                .on('data', (csvrow) => {
                    this.service.formatDataToCSVBySchema(csvrow, schema, false);
                    batchCounter++;
                    ingestionTypeBodyArray.push({...csvrow});
                    if (batchCounter > batchLimit) {
                        batchCounter = 0;
                        csvReadStream.pause();
                        if (ingestionTypeBodyArray.length > 0) {
                            for (let record of ingestionTypeBodyArray) {
                                const isValidSchema: any = this.service.ajvValidator(schema, record);
                                if (isValidSchema.errors) {
                                    record['error_description'] = isValidSchema.errors;
                                    invalidArray.push(record);
                                    errorCounter = errorCounter + 1;
                                } else {
                                    validArray.push(this.service.formatDataToCSVBySchema(record, schema));
                                    validCounter = validCounter + 1;
                                }
                                ingestionTypeBodyArray = []
                            }
                        }
                    }
                })
                .on('error', async (err) => {
                    // delete the file
                    try {
                        await fs.unlinkSync(fileCompletePath);
                        await this.service.deleteLocalFile(`./error-files/${ingestionName}_${fileTrackerPid}_errors.csv`);
                        const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, 'Error');
                        await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        resolve(`Error -> file stream error ${err}`);
                    } catch (e) {
                        console.error('csvImport.service.file delete error: ', e);
                    }
                })
                .on('end', async () => {
                    try {
                        // flush the remaining csv data to API
                        if (ingestionTypeBodyArray.length > 0) {
                            for (let record of ingestionTypeBodyArray) {
                                const isValidSchema: any = this.service.ajvValidator(schema, record);
                                if (isValidSchema.errors) {
                                    record['error_description'] = isValidSchema.errors;
                                    invalidArray.push(record);
                                    errorCounter = errorCounter + 1;
                                } else {
                                    await validArray.push(await this.service.formatDataToCSVBySchema(record, schema));
                                    validCounter = validCounter + 1;
                                }
                            }
                            let file;
                            if (invalidArray) {
                                file = `./error-files/` + ingestionName + '_errors.csv';
                                await this.service.writeToCSVFile(file, invalidArray);
                            }
                            if (validArray) {
                                file = `./emission-files/` + ingestionName + '.csv';
                                await this.service.writeToCSVFile(file, validArray);
                            }
                            const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, 'Uploaded', ingestionName);
                            await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        }
                        resolve({message: 'Success', validCounter: validCounter, invalidCounter: errorCounter});
                    } catch (apiErr) {
                        apiResponseDataList = undefined;
                        let apiErrorData: any = {};
                        apiErrorData = JSON.parse(apiErr.message);
                        const queryStr = await IngestionDatasetQuery.updateFileTracker(fileTrackerPid, `Error ->${apiErrorData.message}`);
                        await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        // delete the file
                        fs.unlinkSync(fileCompletePath);
                        await this.service.deleteLocalFile(`./error-files/${ingestionName}_errors.csv`);
                        resolve(`Error END -> API err ${apiErr.message}`);
                    }
                });
        });
    }
}
