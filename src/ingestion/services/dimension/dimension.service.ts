import {Injectable} from '@nestjs/common';
import {IngestionDatasetQuery} from '../../query/ingestionQuery';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';

@Injectable()
export class DimensionService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction) {
    }

    async createDimension(inputData) {
        try {
            if (inputData.dimension_name) {
                const dimensionName = inputData.dimension_name;
                const queryStr = await IngestionDatasetQuery.getDimension(dimensionName);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    let errorCounter = 0, validCounter = 0;
                    let validArray = [], invalidArray = [];
                    if (inputData.dimension && inputData.dimension.length > 0) {
                        for (let record of inputData.dimension) {
                            const isValidSchema: any = await this.service.ajvValidator(queryResult[0].dimension_data.input.properties.dimension.items, record);
                            if (isValidSchema.errors) {
                                record['description'] = isValidSchema.errors;
                                invalidArray.push(record);
                                errorCounter = errorCounter + 1;
                            } else {
                                let schema = queryResult[0].dimension_data.input.properties.dimension;
                                validArray.push(await this.service.formatDataToCSVBySchema(record, schema));
                                validCounter = validCounter + 1;
                            }
                        }

                        let fileName = dimensionName;
                        if (inputData?.file_tracker_pid) {
                            fileName = dimensionName + `_${inputData?.file_tracker_pid}`;
                        }

                        if (invalidArray.length > 0) {
                            await this.service.writeToCSVFile(`./error-files/` + fileName + '_errors.csv', invalidArray);
                        }
                        if (validArray.length > 0) {

                            await this.service.writeToCSVFile(`./input-files/` + fileName + '.csv', validArray);
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
