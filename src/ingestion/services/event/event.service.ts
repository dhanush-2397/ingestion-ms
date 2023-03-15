import {Injectable} from '@nestjs/common';
import {IngestionDatasetQuery} from '../../query/ingestionQuery';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';

import {uploadToS3} from '../minio-upload'

@Injectable()
export class EventService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction) {
    }

    async createEvent(inputData) {
        try {
            if (inputData.event_name) {
                const eventName = inputData.event_name;
                let queryStr = await IngestionDatasetQuery.getEvents(eventName);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    let errorCounter = 0, validCounter = 0;
                    let validArray = [], invalidArray = [];
                    if (inputData.event && inputData.event.length > 0) {
                        for (let record of inputData.event) {
                            const isValidSchema: any = await this.service.ajvValidator(queryResult[0].schema, record);
                            if (isValidSchema.errors) {
                                record['description'] = isValidSchema.errors;
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
                            fileName = eventName + `_${inputData?.file_tracker_pid}`;
                        }
                        let file;
                        let folderName = await this.service.getDate();
                        if (invalidArray.length > 0) {
                            file = `./error-files/` + fileName + '_errors.csv';
                            await this.service.writeToCSVFile(file, invalidArray);
                            await uploadToS3(`${process.env.ERROR_BUCKET}`, file, fileName + '_errors.csv', `${eventName}/${folderName}`);
                            if (inputData?.file_tracker_pid) {
                                queryStr = await IngestionDatasetQuery.updateCounter(inputData.file_tracker_pid, '', errorCounter);
                                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                            }
                            await this.service.deleteLocalFile(file);
                        }
                        if (validArray.length > 0) {
                            file = `./input-files/` + fileName + '.csv';
                            await this.service.writeToCSVFile(file, validArray);
                            await uploadToS3(`${process.env.INPUT_BUCKET}`, file, fileName + '.csv', `${eventName}/${folderName}`);
                            if (inputData?.file_tracker_pid) {
                                queryStr = await IngestionDatasetQuery.updateCounter(inputData.file_tracker_pid, validCounter, '');
                                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                            }
                            await this.service.deleteLocalFile(file);
                        }
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
}