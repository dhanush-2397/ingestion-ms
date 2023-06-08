import {Test, TestingModule} from '@nestjs/testing';
import {DatabaseService} from '../../../database/database.service';
import {GenericFunction} from '../generic-function';
import {UpdateFileStatusService} from './update-file-status.service';

describe('MyService', () => {
    let service: UpdateFileStatusService;
    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            providers: [DatabaseService, UpdateFileStatusService, GenericFunction,
                {
                    provide: DatabaseService,
                    useValue: {
                        executeQuery: jest.fn().mockReturnValueOnce([]).mockReturnValueOnce([{file_status: "Completed"}])
                    }
                },
                {
                    provide: UpdateFileStatusService,
                    useClass: UpdateFileStatusService
                },
                {
                    provide: GenericFunction,
                    useClass: GenericFunction
                }],
        }).compile();
        service = module.get<UpdateFileStatusService>(UpdateFileStatusService);
    });

    it('should be defined', () => {
        expect(service).toBeDefined();
    });

    // it('validation', async () => {
    //     let input = {
    //         "file_name": "event_students_attendance.csv",
    //         "ingestion_type": "event1",
    //         "ingestion_name": "event_students_attendance",
    //         "status": "Processing"
    //     };

    //     let result = {
    //         code: 400, error: [
    //             {
    //                 "instancePath": "/ingestion_type",
    //                 "schemaPath": "#/properties/ingestion_type/enum",
    //                 "keyword": "enum",
    //                 "params": {
    //                     "allowedValues": [
    //                         "event",
    //                         "dataset",
    //                         "dimension"
    //                     ]
    //                 },
    //                 "message": "must be equal to one of the allowed values"
    //             }
    //         ]
    //     };

    //     expect(await service.UpdateFileStatus(input)).toStrictEqual(result)
    // });

    // it('No file exists with the given details', async () => {
    //     let input = {
    //         "file_name": "event_students_attendance.csv",
    //         "ingestion_type": "event",
    //         "ingestion_name": "event_students_attendance",
    //         "status": "Processing"
    //     };

    //     let result = {code: 400, error: 'No file exists with the given details'};

    //     expect(await service.UpdateFileStatus(input)).toStrictEqual(result)
    // });

    // it('File status updated successfully ready_to_archive: false', async () => {
    //     const module: TestingModule = await Test.createTestingModule({
    //         providers: [DatabaseService, UpdateFileStatusService, GenericFunction,
    //             {
    //                 provide: DatabaseService,
    //                 useValue: {
    //                     executeQuery: jest.fn().mockReturnValueOnce([{file_status: "Completed"}])
    //                 }
    //             },
    //             {
    //                 provide: UpdateFileStatusService,
    //                 useClass: UpdateFileStatusService
    //             },
    //             {
    //                 provide: GenericFunction,
    //                 useClass: GenericFunction
    //             }],
    //     }).compile();
    //     service = module.get<UpdateFileStatusService>(UpdateFileStatusService);
    //     let input = {
    //         "file_name": "event_students_attendance.csv",
    //         "ingestion_type": "event",
    //         "ingestion_name": "event_students_attendance",
    //         "status": "Processing"
    //     };
    //
    //     let result = {
    //         code: 200,
    //         message: "File status updated successfully",
    //         ready_to_archive: false
    //     };
    //
    //     expect(await service.UpdateFileStatus(input)).toStrictEqual(result)
    // });
    //
    // it('File status updated successfully ready_to_archive: true', async () => {
    //     const module: TestingModule = await Test.createTestingModule({
    //         providers: [DatabaseService, UpdateFileStatusService, GenericFunction,
    //             {
    //                 provide: DatabaseService,
    //                 useValue: {
    //                     executeQuery: jest.fn().mockReturnValueOnce([{
    //                         file_status: "Completed",
    //                         pid: 1
    //                     }]).mockReturnValueOnce([])
    //                         .mockReturnValueOnce([{dataset_count: 1}]).mockReturnValueOnce([{dataset_count: 1}]).mockReturnValueOnce([{success_count: 1}])
    //                 }
    //             },
    //             {
    //                 provide: UpdateFileStatusService,
    //                 useClass: UpdateFileStatusService
    //             },
    //             {
    //                 provide: GenericFunction,
    //                 useClass: GenericFunction
    //             }],
    //     }).compile();
    //     service = module.get<UpdateFileStatusService>(UpdateFileStatusService);
    //     let input = {
    //         "file_name": "event_students_attendance.csv",
    //         "ingestion_type": "event",
    //         "ingestion_name": "event_students_attendance",
    //         "status": "Completed"
    //     };
    //
    //     let result = {
    //         code: 200,
    //         message: "File status updated successfully",
    //         ready_to_archive: true
    //     };
    //
    //     expect(await service.UpdateFileStatus(input)).toStrictEqual(result)
    // });
    //
    // it('File status updated successfully', async () => {
    //     const module: TestingModule = await Test.createTestingModule({
    //         providers: [DatabaseService, UpdateFileStatusService, GenericFunction,
    //             {
    //                 provide: DatabaseService,
    //                 useValue: {
    //                     executeQuery: jest.fn().mockReturnValueOnce([{
    //                         file_status: "Completed",
    //                         pid: 1
    //                     }]).mockReturnValueOnce([])
    //                         .mockReturnValueOnce([{dataset_count: 1}]).mockReturnValueOnce([{dataset_count: 1}]).mockReturnValueOnce([{success_count: 1}])
    //                 }
    //             },
    //             {
    //                 provide: UpdateFileStatusService,
    //                 useClass: UpdateFileStatusService
    //             },
    //             {
    //                 provide: GenericFunction,
    //                 useClass: GenericFunction
    //             }],
    //     }).compile();
    //     service = module.get<UpdateFileStatusService>(UpdateFileStatusService);
    //     let input = {
    //         "file_name": "dimension_district.csv",
    //         "ingestion_type": "dimension",
    //         "ingestion_name": "dimension_district",
    //         "status": "Completed"
    //     };
    //
    //     let result = {
    //         code: 200,
    //         message: "File status updated successfully",
    //         ready_to_archive: true
    //     };
    //
    //     expect(await service.UpdateFileStatus(input)).toStrictEqual(result)
    // });

    // it('exception', async () => {
    //     const module: TestingModule = await Test.createTestingModule({
    //         providers: [DatabaseService, UpdateFileStatusService, GenericFunction,
    //             {
    //                 provide: DatabaseService,
    //                 useValue: {
    //                     executeQuery: jest.fn().mockImplementation(() => {
    //                         throw Error("exception test")
    //                     })
    //                 }
    //             },
    //             {
    //                 provide: UpdateFileStatusService,
    //                 useClass: UpdateFileStatusService
    //             },
    //             {
    //                 provide: GenericFunction,
    //                 useClass: GenericFunction
    //             }],
    //     }).compile();
    //     service = module.get<UpdateFileStatusService>(UpdateFileStatusService);
    //     let input = {
    //         "file_name": "event_students_attendance.csv",
    //         "ingestion_type": "event",
    //         "ingestion_name": "event_students_attendance",
    //         "status": "Completed"
    //     };

    //     let resultOutput = "Error: exception test";

    //     try {
    //         await service.UpdateFileStatus(input);
    //     } catch (e) {
    //         expect(e.message).toEqual(resultOutput);
    //     }
    // });
});