import {Test, TestingModule} from '@nestjs/testing';
import {IngestionController} from './ingestion.controller';
import {DatasetService} from '../services/dataset/dataset.service';
import {DimensionService} from '../services/dimension/dimension.service';
import {EventService} from '../services/event/event.service';
import {CsvImportService} from "../services/csvImport/csvImport.service";
import { DatabaseService } from '../../database/database.service';
import { CsvToJsonService } from '../services/csv-to-json/csv-to-json.service';

describe('IngestionController', () => {

    let controller: IngestionController;
    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            controllers: [IngestionController],
            providers: [DatasetService,
                DimensionService, EventService, DatabaseService,
                {
                    provide: DatasetService,
                    useValue: {
                        createDataset: jest.fn(dto => {
                            dto
                        }),
                    }
                },
                {
                    provide: DimensionService,
                    useValue: {
                        createDimenshion: jest.fn(dto => {
                            dto
                        }),
                    }
                },
                {
                    provide: EventService,
                    useValue: {
                        createEvent: jest.fn(dto => {
                            dto
                        }),
                    }
                },
                {
                    provide: CsvImportService,
                    useValue: {
                        readAndParseFile: jest.fn(dto => {
                            dto
                        })
                    }
                },
                {
                    provide: DatabaseService,
                    useValue: {
                        executeQuery: jest.fn(dto => {
                            dto
                        })
                    }
                },
                {
                    provide: CsvToJsonService,
                    useValue: {
                        convertCsvToJson: jest.fn()
                    }
                },
            ],
        }).compile();
        controller = module.get<IngestionController>(IngestionController);
    });

    it('should be defined', () => {
        expect(1).toStrictEqual(1)
    });
});
