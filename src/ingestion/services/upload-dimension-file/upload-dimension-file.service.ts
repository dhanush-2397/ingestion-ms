import { HttpCustomService } from './../HttpCustomService';
import { Injectable } from '@nestjs/common';
const fs = require('fs');
const FormData = require('form-data');

@Injectable()
export class UploadDimensionFileService {
    constructor(private httpService:HttpCustomService){}

    async  uploadFiles(){
        let folderPath = './dimension-files'
        try{
            let result = await this.httpService.get(process.env.URL + '/generatejwt')
            let token: any = result?.data;
            let res = await this.callCsvImportAPI(token, folderPath)
            return res
        }catch(error){
            return {code:400, error:error}
        }
    }
    async callCsvImportAPI(data: string, folderPath:string){
        const files = fs.readdirSync(folderPath);
        let promises = [];
        for(let i=0;i<files?.length;i++){
            const filePath = folderPath + '/' +files[i]
            const fileName: string = files[i]?.split('-')[0]
            console.log("The file name is:", fileName)
            const formData = new FormData();
            formData.append('ingestion_type','dimension');
            formData.append('ingestion_name',`${fileName}`);
            formData.append('file',fs.createReadStream(filePath));
            try{
                let url = `${process.env.URL}` + '/new_programs';        
                const headers = {
                        Authorization: 'Bearer'+ ' '+data,
                        
                    };
                promises.push(this.httpService.post(url,formData,{headers: headers}))
            
            }catch(error){
                console.error("error during csv import:", error)
            }
        }
        try{
            await Promise.all(promises)
            return { code: 200, message:"Uploading the files"}
        }catch(error){
            console.error("error is", error)
            return {code:400, error:"Error occured during the upload"}
        }

    }
}
