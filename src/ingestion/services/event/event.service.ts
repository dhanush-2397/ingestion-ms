import {Injectable} from '@nestjs/common';
import {IngestionDatasetQuery} from '../../query/ingestionQuery';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';
import {IEvent} from '../../interfaces/Ingestion-data'

@Injectable()
export class EventService {
    constructor(private DatabaseService: DatabaseService, private service: GenericFunction) {
    }

    async createEvent(inputData: IEvent) {
        try {
            if (inputData.event_name) {

                const eventName = inputData.event_name;
                const queryStr = await IngestionDatasetQuery.getEvents(eventName);
                const queryResult = await this.DatabaseService.executeQuery(queryStr.query, queryStr.values);
                if (queryResult?.length === 1) {
                    const isValidSchema: any = await this.service.ajvValidator(queryResult[0].event_data.input, inputData);
                    if (isValidSchema.errors) {
                        return {
                            code: 400,
                            error: isValidSchema.errors
                        }
                    } else {
                        let fileName = eventName;
                        if (inputData?.file_tracker_pid) {
                            fileName = eventName + `_${inputData?.file_tracker_pid}`;
                        }
                        let schema = queryResult[0].event_data.input.properties.event;
                        let input = inputData.event;

                        let processedInput = [];
                        processedInput.push(await this.service.processInput(input, schema));
                        console.log('event.service.createEvent: ', inputData.event);
                        await this.service.writeToCSVFile(fileName, inputData.event);
                        return {
                            code: 200,
                            message: "Event added successfully"
                        }
                    }
                } else {
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