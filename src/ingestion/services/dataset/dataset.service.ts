import {HttpStatus, Injectable, Res} from '@nestjs/common';
import {IngestionDatasetQuery} from '../../query/ingestionQuery';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';
import {UploadService} from '../file-uploader-service'

@Injectable()
export class DatasetService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction, private uploadService: UploadService) {
    }

    async createDataset(inputData) {
        try {
            if (inputData.dataset_name) {
                const datasetName = inputData.dataset_name;
                let queryStr = await IngestionDatasetQuery.getDataset(datasetName);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    if (inputData.dataset) {
                        let errorCounter = 0, validCounter = 0;
                        let validArray = [], invalidArray = [];
                        if (inputData.dataset && inputData.dataset.length > 0) {
                            for (let record of inputData.dataset) {
                                const isValidSchema: any = await this.service.ajvValidator(queryResult[0].schema, [record]);
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
                            let file;
                            let fileName = datasetName;
                            if (inputData?.file_tracker_pid) {
                                fileName = datasetName + `_${inputData?.file_tracker_pid}`;
                            }
                            let folderName = await this.service.getDate();
                            if (invalidArray.length > 0) {
                                file = `./error-files/` + fileName + '_errors.csv';
                                await this.service.writeToCSVFile(file, invalidArray);

                                if (process.env.STORAGE_TYPE === 'local') {
                                    await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, file, `ingestion-error/${datasetName}/${folderName}/`);
                                } else if (process.env.STORAGE_TYPE === 'azure') {
                                    await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, file, `ingestion-error/${datasetName}/${folderName}/`);
                                } else {
                                    await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, file, `ingestion-error/${datasetName}/${folderName}/`);
                                }

                                if (inputData?.file_tracker_pid) {
                                    queryStr = await IngestionDatasetQuery.updateCounter(inputData.file_tracker_pid, '', errorCounter);
                                    await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                                }
                            }
                            if (validArray.length > 0) {
                                file = `./input-files/` + fileName + '.csv';
                                await this.service.writeToCSVFile(file, validArray);

                                if (process.env.STORAGE_TYPE === 'local') {
                                    await this.uploadService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, file, `combined-input/${datasetName}/${folderName}/`);
                                } else if (process.env.STORAGE_TYPE === 'azure') {
                                    await this.uploadService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, file, `combined-input/${datasetName}/${folderName}/`);
                                } else {
                                    await this.uploadService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, file, `combined-input/${datasetName}/${folderName}/`);
                                }

                                if (inputData?.file_tracker_pid) {
                                    queryStr = await IngestionDatasetQuery.updateCounter(inputData.file_tracker_pid, validCounter, '');
                                    await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                                }
                            }
                            invalidArray = undefined;
                            validArray = undefined;
                            return {
                                code: 200,
                                message: "Dataset added successfully",
                                errorCounter: errorCounter,
                                validCounter: validCounter
                            }
                        } else {
                            return {
                                code: 400,
                                error: "Items array is required and cannot be empty"
                            }
                        }
                    }
                    else {
                        return {
                            code: 400,
                            error: "Dataset object is required"
                        }
                    }
                }
                else {
                    return {
                        code: 400,
                        error: "No dataset found"
                    }
                }
            } else {
                return {
                    code: 400,
                    error: "Dataset name is missing"
                }
            }
        } catch (e) {
            console.error('create-dataset executeQueryAndReturnResults: ', e.message);
            throw new Error(e);
        }
    }
}
