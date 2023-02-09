import {Injectable} from "@nestjs/common";
import {GenericFunction} from "../generic-function";
import {DatabaseService} from "../../../database/database.service";
import {IngestionDatasetQuery} from "../../query/ingestionQuery";

interface FileStatusInterface {
    file_name: string;
    ingestion_type: string;
    ingestion_name: string;
    status: string;
}

@Injectable()
export class UpdateFileStatusService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction,) {
    }

    async UpdateFileStatus(inputData: FileStatusInterface) {
        try {
            let schema = {
                "type": "object",
                "properties": {
                    "file_name": {
                        "type": "string",
                        "pattern": "^.*\.(csv)$",
                        "shouldnotnull": true
                    },
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
                    },
                    "status": {
                        "type": "string",
                        "shouldnotnull": true
                    }

                },
                "required": [
                    "file_name",
                    "ingestion_type",
                    "ingestion_name",
                    "status"
                ]
            };
            const isValidSchema: any = await this.service.ajvValidator(schema, inputData);
            if (isValidSchema.errors) {
                return {code: 400, error: isValidSchema.errors};
            } else {
                let queryStr = await IngestionDatasetQuery.getFile(inputData.file_name, inputData.ingestion_type, inputData.ingestion_name);
                let queryResult: any = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    let successCount, datasetCount;
                    const fileTrackerPid = queryResult[0].pid;
                    //check the status
                    if (queryResult.file_status !== 'Upload_in_progress' && queryResult.file_status !== 'Error' && queryResult.file_status !== 'Ready_to_archive') {
                        queryStr = await IngestionDatasetQuery.updateFileStatus(fileTrackerPid, inputData.status);
                        await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                        if ((inputData.status).substring(0, 9) === 'Completed' && inputData.ingestion_type === 'event') {
                            //Update File Status
                            queryStr = await IngestionDatasetQuery.updateFilePipelineTracker((inputData.status).substring(10), inputData.file_name, inputData.ingestion_name);
                            queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                            if (queryResult.length === 1) {
                                //Get Total Dataset count
                                queryStr = await IngestionDatasetQuery.getDatasetCount(inputData.ingestion_name);
                                queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                                datasetCount = queryResult[0].dataset_count;

                                //Get Total Success count
                                queryStr = await IngestionDatasetQuery.getSuccessStatusCount(inputData.file_name, inputData.ingestion_name);
                                queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                                successCount = queryResult[0].success_count;

                                if (datasetCount === successCount) {
                                    queryStr = await IngestionDatasetQuery.updateFileStatus(fileTrackerPid, 'Ready_to_archive');
                                    await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                                }
                            } else {
                                return {
                                    code: 400,
                                    error: "Invalid Status"
                                }
                            }
                        } else {
                            if ((inputData.status).substring(0, 9) === 'Completed' && inputData.ingestion_type !== 'event') {
                                queryStr = await IngestionDatasetQuery.updateFileStatus(fileTrackerPid, 'Ready_to_archive');
                                await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                            }
                        }
                    }
                    if ((inputData.ingestion_type === 'dimension' && (inputData.status).substring(0, 9) === 'Completed') || (inputData.ingestion_type === 'dataset' && (inputData.status).substring(0, 9) === 'Completed')
                        || (datasetCount !== undefined && successCount !== undefined) && datasetCount == successCount) {
                        return {
                            code: 200,
                            message: "File status updated successfully",
                            ready_to_archive: true
                        }
                    } else {
                        return {
                            code: 200,
                            message: "File status updated successfully",
                            ready_to_archive: false
                        }
                    }
                } else {
                    return {code: 400, error: 'No file exists with the given details'}
                }
            }
        } catch (e) {
            console.error('update-file-status.service.getFileStatus: ', e.message);
            throw new Error(e);
        }
    }
}