import {HttpStatus, Injectable, Res} from '@nestjs/common';
import {IngestionDatasetQuery} from '../../query/ingestionQuery';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';

@Injectable()
export class DatasetService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction) {
    }

    async createDataset(inputData) {
        try {
            if (inputData.dataset_name) {
                const datasetName = inputData.dataset_name;
                const queryStr = await IngestionDatasetQuery.getDataset(datasetName);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    if (inputData.dataset) {
                        let errorCounter = 0, validCounter = 0;
                        let validArray = [], invalidArray = [];
                        if (inputData.dataset.items && inputData.dataset.items.length > 0) {
                            for (let record of inputData.dataset?.items) {
                                const isValidSchema: any = await this.service.ajvValidator(queryResult[0].dataset_data.input.properties.dataset.properties.items, [record]);
                                if (isValidSchema.errors) {
                                    record['description'] = isValidSchema.errors;
                                    invalidArray.push(record);
                                    errorCounter = errorCounter + 1;
                                } else {
                                    let schema = queryResult[0].dataset_data.input.properties.dataset.properties.items;
                                    validArray.push(await this.service.addQuotes(record, schema));
                                    validCounter = validCounter + 1;
                                }
                            }
                            if (invalidArray.length > 0) {
                                await this.service.writeToCSVFile(`./error-files/` + datasetName + '_errors.csv', invalidArray);
                            }
                            if (validArray.length > 0) {
                                await this.service.writeToCSVFile(`./input-files/` + datasetName + '.csv', validArray);
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
                            error: "dataset object is required"
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
