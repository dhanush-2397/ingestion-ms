import { Test, TestingModule } from '@nestjs/testing';
import { DatabaseService } from '../../../database/database.service';
import { GenericFunction } from '../generic-function';
import { FileStatusService } from './file-status.service';

describe('FileStatusService', () => {
  let service: FileStatusService;
  const mockDatabaseService = {
    executeQuery: jest.fn().mockReturnValue([])
  };
  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DatabaseService, FileStatusService, GenericFunction,
        {
          provide: DatabaseService,
          useValue: mockDatabaseService
        },
        {
          provide: FileStatusService,
          useClass: FileStatusService
        },
        {
          provide: GenericFunction,
          useClass: GenericFunction
        }],
    }).compile();

    service = module.get<FileStatusService>(FileStatusService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('File name cannot be empty', async () => {
    let data = {
      "filename":"asd",
      "ingestion_type":"event",
      "ingestion_name":"ss"
    }
    let result = {
      code: 400, error: [
        {
          "instancePath": "/filename",
          "schemaPath": "#/properties/filename/pattern",
          "keyword": "pattern",
          "params": {
              "pattern": "^.*.(csv)$"
          },
          "message": "must match pattern \"^.*.(csv)$\""
        }
      ]
    }
    expect(await service.getFileStatus(data)).toStrictEqual(result);
  });

  it('No records found', async () => {
    let data = {
      filename: "gggg.csv",
      ingestion_type: "event",
      ingestion_name: "asssd"
    }
    let result = {
      "code": 400, "error": "No records found"
    }
    expect(await service.getFileStatus(data)).toStrictEqual(result)
  });

  it('Get the file status', async () => {

    const Mockdata = {
      pid: 1,
      file_status: "sdj",
      created_at: "2023-01-27T10:11:59.986Z"
    }
    const mockDatabaseService1 = {
      executeQuery: jest.fn().mockReturnValue([Mockdata])
    };
    const module: TestingModule = await Test.createTestingModule({
      providers: [DatabaseService, FileStatusService, GenericFunction,
        {
          provide: DatabaseService,
          useValue: mockDatabaseService1
        },
        {
          provide: FileStatusService,
          useClass: FileStatusService
        },
        {
          provide: GenericFunction,
          useClass: GenericFunction
        }],
    }).compile();
    service = module.get<FileStatusService>(FileStatusService);


    let data = {
      filename: "gggg.csv",
      ingestion_type: "event",
      ingestion_name: "asssd"
    }
    let result = {
      "code": 200, "response": [
        {
          "pid": 1,
          "file_status": "sdj",
          created_at: "2023-01-27T10:11:59.986Z"

        }
      ]
    }
    expect(await service.getFileStatus(data)).toStrictEqual(result)
  });

  it("exception", async () => {

   
    const mockError = {
      executeQuery: jest.fn().mockImplementation(() => {
        throw Error("exception test")
      })
    };
    const module: TestingModule = await Test.createTestingModule({
      providers: [DatabaseService, FileStatusService, GenericFunction,
        {
          provide: DatabaseService,
          useValue: mockError
        },
        {
          provide: FileStatusService,
          useClass: FileStatusService
        },
        {
          provide: GenericFunction,
          useClass: GenericFunction
        }],
    }).compile();
    service = module.get<FileStatusService>(FileStatusService);

    let data = {
      filename: "gggg.csv",
      ingestion_type: "event",
      ingestion_name: "asssd"
    }
    let resultOutput = "Error: exception test";

    try {
      await service.getFileStatus(data);
    } catch (e) {
      expect(e.message).toEqual(resultOutput);
    }
  });


});
