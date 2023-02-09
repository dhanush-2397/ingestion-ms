import {Test, TestingModule} from '@nestjs/testing';
import {GenericFunction} from '../generic-function';
import {CsvImportService} from './csvImport.service';
import {HttpCustomService} from "../HttpCustomService";
import * as fs from 'fs';
import {DatabaseService} from '../../../database/database.service';
import {Readable} from 'stream';

describe('csvImportService', () => {
    let service: CsvImportService;

    const mockHttpservice = {
        post: jest.fn()
    };
    const mockDatabaseService = {
        executeQuery: jest.fn().mockReturnValueOnce([]).mockReturnValueOnce([{pid: 1}])
    }
    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            providers: [HttpCustomService, GenericFunction, CsvImportService, DatabaseService,
                {
                    provide: HttpCustomService,
                    useValue: mockHttpservice
                },
                {
                    provide: GenericFunction,
                    useClass: GenericFunction
                },
                {
                    provide: DatabaseService,
                    useValue: mockDatabaseService
                },
                {
                    provide: CsvImportService,
                    useClass: CsvImportService
                }],
        }).compile();
        service = module.get<CsvImportService>(CsvImportService);
    });

    it('should be defined', () => {
        expect(service).toBeDefined();
    });

    it('Validation Error', async () => {
        const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 3, 'file_validation_error.csv');
        let file1: Express.Multer.File = {
            originalname: 'file_validation_error.csv',
            mimetype: 'text/csv',
            path: filePath,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''
        };

        const inputData = {
            "ingestion_type": "test",
            "ingestion_name": "student_attendance"
        };
        let resultOutput = {
            code: 400,
            error: [
                {
                    instancePath: '/ingestion_type',
                    schemaPath: '#/properties/ingestion_type/enum',
                    keyword: 'enum',
                    params: {
                        allowedValues: [
                            "event",
                            "dataset",
                            "dimension"
                        ]
                    },
                    message: 'must be equal to one of the allowed values'
                }
            ]
        };
        try {
            await service.readAndParseFile(inputData, file1);
        } catch (e) {
            expect(e).toEqual(resultOutput);
        }
        fs.unlinkSync(filePath);
    });

    it('File is not Tracked', async () => {
        const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'list_valid_large.csv');
        let file1: Express.Multer.File = {
            originalname: 'list_valid_large.csv',
            mimetype: 'text/csv',
            path: filePath,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''
        };
        const inputData = {
            "ingestion_type": "event",
            "ingestion_name": "student_attendance"
        };
        let resultOutput = {"code": 400, "error": "File is not Tracked"};
        expect(await service.readAndParseFile(inputData, file1)).toStrictEqual(resultOutput);
        fs.unlinkSync(filePath);
    });

    it('File is being processed', async () => {

        const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'file_in_process.csv');
        let file1: Express.Multer.File = {
            originalname: 'file_in_process.csv',
            mimetype: 'text/csv',
            path: filePath,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''
        };
        const inputData = {
            "ingestion_type": "event",
            "ingestion_name": "student_attendance"
        };

        let resultOutput = {code: 200, message: 'File is being processed'};
        expect(await service.readAndParseFile(inputData, file1)).toStrictEqual(resultOutput);
    });

    // it('CSV Uploaded Successfully', async () => {
    //
    //     const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'file1.csv');
    //     let file1: Express.Multer.File = {
    //         originalname: 'file1.csv',
    //         mimetype: 'text/csv',
    //         path: filePath,
    //         buffer: Buffer.from('one,two,three'),
    //         fieldname: '',
    //         encoding: '',
    //         size: 0,
    //         stream: new Readable,
    //         destination: '',
    //         filename: ''
    //     };
    //     const inputData = {
    //         "ingestion_type": "event",
    //         "ingestion_name": "student_attendance"
    //     };
    //
    //     expect(await service.asyncProcessing(inputData, file1.path, 1));
    // });

    it('should make a successful API call and resume the stream', async () => {
        const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'file_api_call_resume.csv');
        let file1: Express.Multer.File = {
            originalname: 'file_api_call_resume.csv',
            mimetype: 'text/csv',
            path: filePath,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''
        };

        try{
            const csvReadStream = fs.createReadStream(file1.path);
            const mockHttp = {
                post: jest.fn().mockResolvedValue(true)
            };
            const mockCsvReadStream = {
                resume: jest.fn(),
                destroy: jest.fn()
            };
            const module: TestingModule = await Test.createTestingModule({
                providers: [HttpCustomService, GenericFunction, CsvImportService, DatabaseService,
                    {
                        provide: HttpCustomService,
                        useValue: mockHttp
                    },
                    {
                        provide: GenericFunction,
                        useClass: GenericFunction
                    },
                    {
                        provide: DatabaseService,
                        useValue: mockDatabaseService
                    },
                    {
                        provide: CsvImportService,
                        useClass: CsvImportService
                    }],
            }).compile();
            service = module.get<CsvImportService>(CsvImportService);
            csvReadStream.pause();
            await service.resetAndMakeAPICall('dataset', 'student_attendance', ['school_id', 'grade', 'count'], csvReadStream, true);
            expect(mockHttp.post).toHaveBeenCalled();
            expect(mockCsvReadStream.destroy).not.toHaveBeenCalled();
            csvReadStream.destroy();
            // fs.unlinkSync(filePath);
        }catch (e) {
            console.error('csvImport.service.spec.: ', e);
        }

    });

    it('it should make an unsuccessful API call and throw an error', async () => {

        const mockHttp = {
            post: jest.fn().mockRejectedValue({response: {data: 'API error'}})
        };
        const filePath1 = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'file_unsuccessful_api.csv');
        let file1: Express.Multer.File = {
            originalname: 'file_unsuccessful_api.csv',
            mimetype: 'text/csv',
            path: filePath1,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''

        };
        const module: TestingModule = await Test.createTestingModule({
            providers: [HttpCustomService, GenericFunction, CsvImportService, DatabaseService,
                {
                    provide: HttpCustomService,
                    useValue: mockHttp
                },
                {
                    provide: GenericFunction,
                    useClass: GenericFunction
                },
                {
                    provide: DatabaseService,
                    useValue: mockDatabaseService
                },
                {
                    provide: CsvImportService,
                    useClass: CsvImportService
                }],
        }).compile();
        let csvReadStream;
        try{
            csvReadStream = fs.createReadStream(file1.path);
            csvReadStream.pause();
            service = module.get<CsvImportService>(CsvImportService);
            await expect(service.resetAndMakeAPICall('dataset', 'ingestionName', [], csvReadStream, true)).rejects.toThrowError('"API error"');
            csvReadStream.destroy();
            // fs.unlinkSync(filePath1);
        }catch (e) {
            console.error('csvImport.service.spec.file read err: ', e);
        }
    });

    // it('it should make an unsuccessful API call and throw an error in end', async () => {
    //
    //     const mockHttp = {
    //         post: jest.fn().mockRejectedValue({response: {data: {message:'API error'}}})
    //     };
    //     const filePath1 = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 2, 'file_unsuccessful_api_end.csv');
    //     let file1: Express.Multer.File = {
    //         originalname: 'file_unsuccessful_api_end.csv',
    //         mimetype: 'text/csv',
    //         path: filePath1,
    //         buffer: Buffer.from('one,two,three'),
    //         fieldname: '',
    //         encoding: '',
    //         size: 0,
    //         stream: new Readable,
    //         destination: '',
    //         filename: ''
    //
    //     };
    //     const module: TestingModule = await Test.createTestingModule({
    //         providers: [HttpCustomService, GenericFunction, CsvImportService, DatabaseService,
    //             {
    //                 provide: HttpCustomService,
    //                 useValue: mockHttp
    //             },
    //             {
    //                 provide: GenericFunction,
    //                 useClass: GenericFunction
    //             },
    //             {
    //                 provide: DatabaseService,
    //                 useValue: mockDatabaseService
    //             },
    //             {
    //                 provide: CsvImportService,
    //                 useClass: CsvImportService
    //             }],
    //     }).compile();
    //     service = module.get<CsvImportService>(CsvImportService);
    //     const inputData = {
    //         "ingestion_type": "event",
    //         "ingestion_name": "student_attendance"
    //     };
    //
    //     await expect(service.asyncProcessing(inputData, file1.path, 1));
    // });

    it('it should make an unsuccessful API call and destroy', async () => {

        const mockHttp = {
            post: jest.fn().mockRejectedValue({response: {data: 'API error'}})
        };
        const mockCsvReadStream = {
            resume: jest.fn(),
            destroy: jest.fn()
        };
        const filePath1 = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'file_only.csv');
        let file1: Express.Multer.File = {
            originalname: 'file_only.csv',
            mimetype: 'text/csv',
            path: filePath1,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''

        };
        const module: TestingModule = await Test.createTestingModule({
            providers: [HttpCustomService, GenericFunction, CsvImportService, DatabaseService,
                {
                    provide: HttpCustomService,
                    useValue: mockHttp
                },
                {
                    provide: GenericFunction,
                    useClass: GenericFunction
                },
                {
                    provide: DatabaseService,
                    useValue: mockDatabaseService
                },
                {
                    provide: CsvImportService,
                    useClass: CsvImportService
                }],
        }).compile();
        const csvReadStream = fs.createReadStream(file1.path);
        csvReadStream.pause();
        service = module.get<CsvImportService>(CsvImportService);
        await expect(service.resetAndMakeAPICall('dataset', 'ingestionName', [], csvReadStream, false));
        csvReadStream.destroy();
        // fs.unlinkSync(filePath1);
    });

    it('CSV Uploaded Successfully with BatchLimit', async () => {

        const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 100001, 'file_upload_success_with_batch.csv');
        let file1: Express.Multer.File = {
            originalname: 'file_upload_success_with_batch.csv',
            mimetype: 'text/csv',
            path: filePath,
            buffer: Buffer.from('one,two,three'),
            fieldname: '',
            encoding: '',
            size: 0,
            stream: new Readable,
            destination: '',
            filename: ''
        };
        const inputData = {
            "ingestion_type": "event",
            "ingestion_name": "student_attendance"
        };

        await service.asyncProcessing(inputData, file1.path, 1);
    });

    // it('Steam error', async () => {
    //
    //     const mockedFs = fs as jest.Mocked<typeof fs>;
    //
    //     const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 10, 'file_stream_err.csv');
    //     let file1: Express.Multer.File = {
    //         originalname: 'file_stream_err.csv',
    //         mimetype: 'text/csv',
    //         path: filePath,
    //         buffer: Buffer.from('one,two,three'),
    //         fieldname: '',
    //         encoding: '',
    //         size: 0,
    //         stream: new Readable,
    //         destination: '',
    //         filename: ''
    //     };
    //     const inputData = {
    //         "ingestion_type": "event",
    //         "ingestion_name": "student_attendance"
    //     };
    //     try {
    //
    //         const mReadStream: any = {
    //             pipe: jest.fn().mockReturnThis(),
    //             on: jest.fn().mockImplementation(function (event, handler) {
    //                 if (event === 'error') {
    //                     handler(new Error('Test stream error'));
    //                 }
    //                 return this;
    //             }),
    //         };
    //         jest.spyOn(fs, 'createReadStream').mockReturnValueOnce(mReadStream);
    //         mockedFs.createReadStream.mockReturnValueOnce(mReadStream);
    //         await expect(service.asyncProcessing(inputData, file1.path, 1));
    //         expect(fs.createReadStream).toBeCalledTimes(1);
    //         expect(mReadStream.pipe).toBeCalledTimes(1);
    //         expect(mReadStream.on).toBeCalledWith('error', expect.any(Function));
    //
    //
    //     } catch (e) {
    //         console.error('csvImport.service.spec.error: ', e.message);
    //         expect(e.message).toStrictEqual('Test error');
    //
    //     }
    // });
});

async function errorCSVTest(service, filePath) {
    const errorResponse = {
        response: {
            data: {
                "message": [
                    {
                        "instancePath": "/event/0",
                        "schemaPath": "#/properties/event/items/required",
                        "keyword": "required",
                        "params": {
                            "missingProperty": "date"
                        },
                        "message": "must have required property 'date'"
                    }
                ]
            }
        }
    };
    const mockHttpservice = {
        post: jest.fn().mockImplementation(() => {
            throw errorResponse;
        })
    };
    const localModule: TestingModule = await Test.createTestingModule({
        providers: [HttpCustomService, GenericFunction, CsvImportService,
            {
                provide: HttpCustomService,
                useValue: mockHttpservice
            },
            {
                provide: GenericFunction,
                useClass: GenericFunction
            },
            {
                provide: CsvImportService,
                useClass: CsvImportService
            }],
    }).compile();
    service = localModule.get<CsvImportService>(CsvImportService);

    const inputData = {
        "ingestion_type": "event",
        "ingestion_name": "student_attendance"
    };
    let resultOutput = {code: 400, error: errorResponse.response.data.message};
    try {
        // promise reject is an error to be catched
        await service.readAndParseFile(inputData, filePath);
    } catch (someErr) {
        expect(someErr).toStrictEqual(resultOutput);
    }
}

// dynamically create csv file on specified number of line
function createNumberOfLineCSVFile(columns, csvNumberOfLine, fileName) {
    let csvFileData = columns.join(',') + '\n';
    const columnLength = columns.length;
    let individualLine = [];
    for (let i = 0; i < csvNumberOfLine; i++) {
        individualLine = [];
        for (let j = 1; j <= columnLength; j++) {
            individualLine.push((i + j).toString());
        }
        csvFileData += `${individualLine.join(',')}\n`
    }
    const dirName = './files/';
    createFile(dirName, fileName, csvFileData);
    return dirName + fileName;
}

const createFile = (
    path: string,
    fileName: string,
    data: string,
): void => {
    if (!fs.existsSync(path)) {
        fs.mkdirSync(path);
    }
    fs.writeFileSync(`${path}${fileName}`, data, 'utf8');

};
