import { Injectable } from '@nestjs/common';
import { IngestionDatasetQuery } from '../../query/ingestionQuery';
import { DatabaseService } from '../../../database/database.service';
import { GenericFunction } from '../generic-function';
import { UploadService } from '../file-uploader-service';
import { GrammarService } from '../grammar/grammar.service';
import { Request } from 'express';
const fs = require('fs');
const {parse} = require('@fast-csv/parse');

@Injectable()
export class EventService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction, private uploadService: UploadService, private grammarService: GrammarService) {
    }

    async createEvent(inputData) {
        try {            
            if (inputData.event_name) {
                let eventName = inputData.event_name;
                let isEnd = inputData?.isEnd;
                let isTelemetryWritingEnd = inputData?.isTelemetryWritingEnd;
                let queryStr = await IngestionDatasetQuery.getEvents(eventName);                
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                
                if (queryResult?.length === 1) {
                    let errorCounter = 0, validCounter = 0;
                    let validArray = [], invalidArray = [];
                    if (inputData.event && inputData.event.length > 0) {
                        for (let record of inputData.event) {
                            const isValidSchema: any = await this.service.ajvValidator(queryResult[0].schema, record);
                            if (isValidSchema.errors) {
                                record['error_description'] = isValidSchema.errors.map(error => error.instancePath?.slice(1,) + ' ' + error.message);//push the records with error description
                                invalidArray.push(record);
                                errorCounter = errorCounter + 1;
                            } else {
                                let schema = queryResult[0].schema;
                                validArray.push(await this.service.formatDataToCSVBySchema(record, schema));
                                validCounter = validCounter + 1;
                            }
                        }
                        let fileName = eventName;
                        if (inputData?.file_tracker_pid) {
                            fileName = eventName + `-event.data`;
                        }
                        let file;
                        if (inputData?.program_name) {
                            eventName = inputData?.program_name;
                        }
                        let folderName = await this.service.getDate();
                        if (invalidArray.length > 0) {
                            file = `./error-files/` + `${eventName}_${inputData?.file_tracker_pid}_errors.csv`;
                            await this.service.writeToCSVFile(file, invalidArray);
                            if (isEnd) {
                                if (process.env.STORAGE_TYPE === 'local') {
                                    await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, file, `ingestion_error/${eventName}/${folderName}/`);
                                } else if (process.env.STORAGE_TYPE === 'azure') {
                                    await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, file, `ingestion_error/${eventName}/${folderName}/`);
                                } else if (process.env.STORAGE_TYPE === 'oracle') {
                                    await this.uploadService.uploadFiles('oracle', `${process.env.ORACLE_BUCKET}`, file, `ingestion_error/${eventName}/${folderName}/`);
                                } else {
                                    await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, file, `ingestion_error/${eventName}/${folderName}/`);
                                }
                            }
                            if (inputData?.file_tracker_pid) {
                                let errorCountQuery = await IngestionDatasetQuery.getCounter(inputData?.file_tracker_pid)
                                let result = await this.DatabaseService.executeQuery(errorCountQuery.query, errorCountQuery.values);
                                errorCounter = errorCounter + (+ result[0]?.error_data_count);
                                queryStr = await IngestionDatasetQuery.updateCounter(inputData.file_tracker_pid, '', errorCounter);
                                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                            }


                        }
                        
                        if (validArray.length > 0) {
                            if(inputData.event_name == 'telemetry'){
                                file = `./emission-files/` + fileName + '.csv';
                                await this.service.writeTelemetryToCSV(file, validArray);
                            } else {
                                file = `./input-files/` + fileName + '.csv';
                                await this.service.writeToCSVFile(file, validArray);
                            }
                            
                            if(isTelemetryWritingEnd && inputData.event_name == 'telemetry'){
                                let filePath = `./emission-files/` + fileName + ".csv";
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
                                // delete the file
                                await this.service.deleteLocalFile(filePath)
                            }
                            if (isEnd) {
                                if (process.env.STORAGE_TYPE === 'local') {
                                    await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, file, `process_input/${eventName}/${folderName}/`);
                                } else if (process.env.STORAGE_TYPE === 'azure') {
                                    await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, file, `process_input/${eventName}/${folderName}/`);
                                } else if (process.env.STORAGE_TYPE === 'oracle') {
                                    await this.uploadService.uploadFiles('oracle', `${process.env.ORACLE_BUCKET}`, file, `process_input/${eventName}/${folderName}/`);
                                } else {
                                    await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, file, `process_input/${eventName}/${folderName}/`);
                                }
                            }
                            if (inputData?.file_tracker_pid) {
                                let processCountQuery = await IngestionDatasetQuery.getCounter(inputData?.file_tracker_pid)
                                let result = await this.DatabaseService.executeQuery(processCountQuery.query, processCountQuery.values);
                                validCounter = validCounter + (+ result[0]?.processed_data_count);
                                queryStr = await IngestionDatasetQuery.updateCounter(inputData.file_tracker_pid, validCounter, '');
                                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                            }
                        }
                        queryStr = await IngestionDatasetQuery.updateTotalCounter(inputData.file_tracker_pid);
                        await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);

                        invalidArray = undefined;
                        validArray = undefined;
                        return {
                            code: 200,
                            message: "Event added successfully",
                            errorCounter: errorCounter,
                            validCounter: validCounter
                        }
                    } else {
                        return {
                            code: 400,
                            error: "Event array is required and cannot be empty"
                        }
                    }
                }
                else {
                    return {
                        code: 400,
                        error: "No event found"
                    }
                }
            } else {
                return {
                    code: 400,
                    error: "Event name is missing"
                }
            }
        } catch (e) {
            console.error('create-event-impl.executeQueryAndReturnResults: ', e.message);
            throw new Error(e);
        }
    }

    async validateEvent(inputData, file: Express.Multer.File, request?: Request) {
        return new Promise(async (resolve, reject) => {
            const { id } = inputData;
            const fileCompletePath = file.path;
            const fileSize = file.size;
            const uploadedFileName = file.originalname;
            const eventGrammar = await this.grammarService.getEventSchemaByID(+id);

            if (eventGrammar?.length > 0) {
                const schema = eventGrammar[0].schema;
                const rows = [];
                const csvReadStream = fs.createReadStream(fileCompletePath)
                    .pipe(parse({headers: true}))
                    .on('data', async (csvrow) => {
                        this.service.formatDataToCSVBySchema(csvrow, schema, false);
                        const isValidSchema: any = await this.service.ajvValidator(schema, csvrow);
                        if (isValidSchema.errors) {
                            csvrow['error_description'] = isValidSchema.errors;
                        }

                        rows.push(csvrow);
                    })
                    .on('error', async (err) => {
                        // delete the file
                        try {
                            await fs.unlinkSync(fileCompletePath);
                            reject(`Error -> file stream error ${err}`);
                        } catch (e) {
                            console.error('csvImport.service.file delete error: ', e);
                            reject(`csvImport.service.file delete error: ${e}`);
                        }
                    })
                    .on('end', async () => {
                        try {
                            await fs.unlinkSync(fileCompletePath);
                            resolve(rows);
                        } catch (e) {
                            console.error('csvImport.service.file delete error: ', e);
                            reject('csvImport.service.file delete error: ' + e);
                        }
                    });
            } else {
                reject('No grammar found for the given id');
            }
        });
    }
    
    async createTelemetryEvent(inputData) {
        try {            
            if (inputData?.event_name) {
                let eventName = inputData?.event_name;
                let queryStr = await IngestionDatasetQuery.getEvents(eventName);                
                const queryResult = await this.DatabaseService.executeQuery(queryStr?.query, queryStr?.values);
                if (queryResult?.length === 1) {
                    let validArray = [], invalidArray = [];
                    if (inputData.event && inputData.event.length > 0) {
                        for (let record of inputData.event) {                            
                            let schema = queryResult[0].schema;
                            validArray.push(await this.service.formatDataToCSVBySchema(record, schema));
                        }
                        let file;
                        if (validArray.length > 0) {
                            if(!inputData?.isTelemetryWritingEnd){
                                file = `./emission-files/` + eventName + '.csv';
                                await this.service.writeTelemetryToCSV(file, validArray);
                            }
                        }
                        
                        queryStr = await IngestionDatasetQuery.updateTotalCounter(inputData.file_tracker_pid);
                        await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);

                        invalidArray = undefined;
                        validArray = undefined;
                        return {
                            code: 200,
                            message: "Event added successfully"
                        }
                    } else {
                        return {
                            code: 400,
                            error: "Event array is required and cannot be empty"
                        }
                    }
                }
                else {
                    if(inputData?.isTelemetryWritingEnd){
                        let filePath = `./emission-files/` + eventName + ".csv";
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
                        // delete the file
                        await this.service.deleteLocalFile(filePath)
                        return {
                            code: 200,
                            message: "Event data uploaded successfully"
                        }
                    }
                }
            } else {
                return {
                    code: 400,
                    error: "Event name is missing"
                }
            }
        } catch (e) {
            console.error('create-event-impl.executeQueryAndReturnResults: ', e.message);
            throw new Error(e);
        }
    }
}