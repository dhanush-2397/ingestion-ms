import {Injectable} from '@nestjs/common';
import {IngestionDatasetQuery} from '../../query/ingestionQuery';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';
import {uploadToMinio} from '../minio-upload'
import {AzureUpload} from "../azure-upload";

@Injectable()
export class DimensionService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction, private azureService: AzureUpload) {
    }

    async createDimension(inputData) {
        try {
            if (inputData.dimension_name) {
                const dimensionName = inputData.dimension_name;
                let queryStr = await IngestionDatasetQuery.getDimension(dimensionName);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    let errorCounter = 0, validCounter = 0;
                    let validArray = [], invalidArray = [];
                    if (inputData.dimension && inputData.dimension.length > 0) {
                        for (let record of inputData.dimension) {
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

                        let fileName = dimensionName;
                        if (inputData?.file_tracker_pid) {
                            fileName = dimensionName + `_${inputData?.file_tracker_pid}`;
                        }
                        let file;
                        let folderName = await this.service.getDate();
                        if (invalidArray.length > 0) {
                            file = `./error-files/` + fileName + '_errors.csv';
                            await this.service.writeToCSVFile(file, invalidArray);

                            if (process.env.STORAGE_TYPE === 'local') {
                                await uploadToMinio(`${process.env.INGESTION_ERROR_BUCKET}`, file, fileName + '_errors.csv', `${dimensionName}/${folderName}`);
                            } else if (process.env.STORAGE_TYPE === 'azure') {
                                await this.azureService.uploadBlob(`${process.env.INGESTION_ERROR_CONTAINER}`, file, `${dimensionName}/${folderName}/${fileName}_errors.csv`);
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
                                await uploadToMinio(`${process.env.INPUT_BUCKET}`, file, fileName + '.csv', `${dimensionName}/${folderName}`);
                            } else if (process.env.STORAGE_TYPE === 'azure') {
                                await this.azureService.uploadBlob(`${process.env.INPUT_CONTAINER}`, file, `${dimensionName}/${folderName}/${fileName}.csv`);
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
                            message: "Dimension added successfully",
                            errorCounter: errorCounter,
                            validCounter: validCounter
                        }
                    } else {
                        return {
                            code: 400,
                            error: "Dimension array is required and cannot be empty"
                        }
                    }
                } else {
                    return {
                        code: 400,
                        error: "No dimension found"
                    }
                }
            } else {
                return {
                    code: 400,
                    error: "Dimension name is missing"
                }
            }
        } catch (e) {
            console.error('create-dimension-impl.executeQueryAndReturnResults:', e.message);
            throw new Error(e);
        }
    }
}
