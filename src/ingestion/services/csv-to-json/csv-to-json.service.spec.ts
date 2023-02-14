import { Test, TestingModule } from '@nestjs/testing';
import { Readable } from 'typeorm/platform/PlatformTools';
import { CsvToJsonService } from './csv-to-json.service';
import * as fs from 'fs';

describe('CsvToJsonService', () => {
  let service: CsvToJsonService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [CsvToJsonService],
    }).compile();
 
    service = module.get<CsvToJsonService>(CsvToJsonService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  // it("File converted successfully", async () => {
  //   const filePath = createNumberOfLineCSVFile(['school_id', 'grade', 'count'], 1003, 'file_api_call_resume.csv');
  //   let file: Express.Multer.File = {
  //     originalname: 'file_api_call_resume.csv',
  //     mimetype: 'text/csv',
  //     path: filePath,
  //     buffer: Buffer.from('one,two,three'),
  //     fieldname: '',
  //     encoding: '',
  //     size: 0,
  //     stream: new Readable,
  //     destination: '',
  //     filename: ''
  //   };
  //   let result = {
  //     "code": 200,
  //     "message": "File converted successfully",
  //     "response": []
  //   }
  //   expect(await service.convertCsvToJson()).toStrictEqual(result)

  // })

  // function createNumberOfLineCSVFile(columns, csvNumberOfLine, fileName) {
  //   let csvFileData = columns.join(',') + '\n';
  //   const columnLength = columns.length;
  //   let individualLine = [];
  //   for (let i = 0; i < csvNumberOfLine; i++) {
  //     individualLine = [];
  //     for (let j = 1; j <= columnLength; j++) {
  //       individualLine.push((i + j).toString());
  //     }
  //     csvFileData += `${individualLine.join(',')}\n`
  //   }
  //   const dirName = './files/';
  //   createFile(dirName, fileName, csvFileData);
  //   return dirName + fileName;
  // }

  // const createFile = (
  //   path: string,
  //   fileName: string,
  //   data: string,
  // ): void => {
  //   if (!fs.existsSync(path)) {
  //     fs.mkdirSync(path);
  //   }
  //   fs.writeFileSync(`${path}${fileName}`, data, 'utf8');
  // };

});
