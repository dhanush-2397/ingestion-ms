import { DatabaseService } from './../../../database/database.service';
import { GenericFunction } from './../generic-function';
import { UploadService } from './../file-uploader-service';
import { Injectable } from '@nestjs/common';
import axios from 'axios';
import { IngestionDatasetQuery } from 'src/ingestion/query/ingestionQuery';
const csv = require('csv-parser');
const fs = require('fs');
@Injectable()
export class NvskApiService {
   constructor(private fileService: UploadService, private service: GenericFunction, private databaseService: DatabaseService) {

   }
   /* NVSK side implementations */
   async getEmitterData() {

      let urlData = [
         {
            program_name: 'pm_poshan',
            urls: ["https://s3-cqube-edu-1.s3.ap-south-1.amazonaws.com/emission/25-Aug-2023/pm-poshan_access-across-india.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=AKIA2YWRVRZFEVR7OGPL%2F20230825%2Fap-south-1%2Fs3%2Faws4_request&X-Amz-Date=20230825T082056Z&X-Amz-Expires=432000&X-Amz-Signature=929590a8c6523b59dda708577bfbcdd20e02264f2edac13abcb2613525d7f794&X-Amz-SignedHeaders=host&x-id=GetObject"]
         }
      ];

      this.writeRawDataFromUrl(urlData)
      return { code: 200, message: "VSK Writing to the file in process" }
   }
   async writeRawDataFromUrl(urlData: Array<{ program_name: string, urls: string[] }>) {
      try {
         if (urlData?.length > 0) {
            for (let data of urlData) {
               let pgname = data.program_name;
               console.log("The program name is:", pgname);
               for (let url of data.urls) {
                  const parsedUrl = new URL(url);
                  const fileName = `./rawdata-files/` + parsedUrl.pathname.split('/').pop();
                  const stream = (await axios.get(url, { responseType: 'stream' })).data
                  const filteredCsvStream = fs.createWriteStream(`${fileName}`);
                  let isFirstRow = true;
                  stream
                     .pipe(csv({}))
                     .on('data', (row) => {
                        // Filter data based on state_id
                        if (isFirstRow) {
                           filteredCsvStream.write(Object.keys(row).join(',') + '\n');
                           isFirstRow = false;
                        }
                        if (row['state_code'].slice(1, -1) === '12') {
                           filteredCsvStream.write(Object.values(row).join(',') + '\n');
                        }
                     })
                     .on('end', async () => {
                        filteredCsvStream.end();
                        let folderName = await this.service.getDate();
                        try {
                           if (process.env.STORAGE_TYPE == 'local') {
                              await this.fileService.uploadFiles('local', `${process.env.MINIO_BUCKET}`, fileName, `emission/${folderName}/`);
                           } else if (process.env.STORAGE_TYPE === 'azure') {
                              await this.fileService.uploadFiles('azure', `${process.env.AZURE_CONTAINER}`, fileName, `emission/${folderName}/`);
                           } else if (process.env.STORAGE_TYPE === 'oracle') {
                              await this.fileService.uploadFiles('oracle', `${process.env.ORACLE_BUCKET}`, fileName, `emission/${folderName}/`);
                           } else {
                              await this.fileService.uploadFiles('aws', `${process.env.AWS_BUCKET}`, fileName, `emission/${folderName}/`);
                           }
                        } catch (error) {
                           this.service.deleteLocalFile(fileName)
                        }
                        const queryStr = await IngestionDatasetQuery.insertIntoEmission(pgname, url, 'Uploaded')
                        console.log("Query string is:", queryStr);
                        const result = await this.databaseService.executeQuery(queryStr.query, queryStr.values)
                        this.service.deleteLocalFile(fileName)
                        console.log(`Filtered data saved to ${fileName}`);
                        return { code: 200, message: "successfully written to the file" }
                     })
                     .on('error', (error) => {
                        this.service.deleteLocalFile(fileName);
                        console.error('Error processing CSV:', error);
                     });
               }
            }
         }
      } catch (error) {
         console.log("error is:", error);
      }
   }
}






