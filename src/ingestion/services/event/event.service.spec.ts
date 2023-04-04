import {Test, TestingModule} from '@nestjs/testing';
import {GenericFunction} from '../generic-function';
import {EventService} from './event.service';
import {DatabaseService} from '../../../database/database.service';
import * as fs from 'fs';
import {UploadService} from "../file-uploader-service";

describe('EventService', () => {
    let service: EventService;
    const data = {
        "input": {
            "type": "object",
            "required": [
                "event_name",
                "event"
            ],
            "properties": {
                "event": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": [
                            "school_id",
                            "school_name"
                        ],
                        "properties": {
                            "school_id": {
                                "type": "string"
                            },
                            "school_name": {
                                "type": "string"
                            }
                        }
                    }
                },
                "event_name": {
                    "type": "string"
                }
            }
        },
        "event_name": "event",
        "ingestion_type": "event"
    };

    const mockDatabaseService = {
        executeQuery: jest.fn().mockReturnValueOnce(0).mockReturnValueOnce([{event_data: data}])
            .mockReturnValueOnce([{event_data: data}])
            .mockReturnValueOnce([{event_data: data}])
    };
    jest.useFakeTimers();

    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            providers: [DatabaseService, GenericFunction,UploadService,
                {
                    provide: DatabaseService,
                    useValue: mockDatabaseService
                },
                {
                    provide: EventService,
                    useClass: EventService
                },
                {
                    provide: GenericFunction,
                    useClass: GenericFunction
                }],
        }).compile();
        service = module.get<EventService>(EventService);
    });

    it('should be defined', () => {
        expect(service).toBeDefined();
    });

    it('No Event Found', async () => {
        const eventData = {
            "event_name": "district",
            "event": [{
                "name": "jhaha",
                "district_id": "SH123"
            }]
        };
        let resultOutput =
            {code: 400, error: "No event found"};
        expect(await service.createEvent(eventData)).toStrictEqual(resultOutput)
    });

    it('Event Name is Missing', async () => {
        const eventData = {
            "event_name": "",
            "event": [{
                "school_id": "6677",
                "school_name": "test"
            }]
        };

        let resultOutput =
            {code: 400, error: "Event name is missing"};

        expect(await service.createEvent(eventData)).toStrictEqual(resultOutput);

    });
});
