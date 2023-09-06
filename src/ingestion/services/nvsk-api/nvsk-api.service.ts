import { Result } from './../../interfaces/Ingestion-data';
import { HttpCustomService } from './../HttpCustomService';
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
   constructor(private fileService: UploadService, private service: GenericFunction, private databaseService: DatabaseService, private httpService:HttpCustomService) {

   }
   /* NVSK side implementations */
   async getEmitterData() {
      let urlData;
      let names = process.env.PROGRAM_NAMES.split(',')
      let body:any ={
         "program_names":names
      }
      const result = await this.httpService.post(process.env.NVSK_URL + '/getRawData',body)
      if(result?.data['code'] === 200){
         console.log("the result data is:",result?.data['data']);
         urlData = result?.data['data']
      }else{
         console.log(JSON.stringify(result.data));
         return {code:400,error:result?.data['error']?result?.data['error']:"Error occured during the NVSK data emission"}
      }
      console.log("the url data is:", urlData);
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
                  console.log("The filename is:",fileName, url);
                  const stream = (await axios.get(url, { responseType: 'stream' })).data
                  const filteredCsvStream = fs.createWriteStream(`${fileName}`);
                  let isFirstRow = true;
                  stream
                     .pipe(csv({}))
                     .on('data', (row) => {
                        if (isFirstRow) {
                           filteredCsvStream.write(Object.keys(row).join(',') + '\n');
                           isFirstRow = false;
                        }
                        if (row['state_code'].slice(1, -1) === process.env.STATE_CODE) {
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
                        const result = await this.databaseService.executeQuery(queryStr.query, queryStr.values)
                        // this.service.deleteLocalFile(fileName)
                        console.log(`Filtered data saved to ${fileName}`);
                        return { code: 200, message: "successfully written to the file" }
                     })
                     .on('error', async (error) => {
                        const queryStr = await IngestionDatasetQuery.insertIntoEmission(pgname, url, error)
                        const result = await this.databaseService.executeQuery(queryStr.query, queryStr.values)
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






