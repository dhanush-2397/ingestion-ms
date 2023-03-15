const AWS = require("aws-sdk"); // from AWS SDK
const fs = require("fs"); // from node.js
const path = require("path"); // from node.js
import { Result } from "../interfaces/Ingestion-data";
import {GenericFunction} from "../services/generic-function";
// configuration
const config = {
  s3BucketName: process.env.AWS_EMISSION_BUCKET,
  folderPath: '../../../emission-files' 
};
let deleteFile = new GenericFunction()
const s3 = new AWS.S3({ 
    signatureVersion: 'v4',
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY
  });

// resolve full folder path
export async function uploadFilestoS3(bucketName:string, folderPath:string, fileName?:string): Promise<Result> {
 return  new Promise(async (resolve, reject)=>{
    let result:boolean;
    const distFolderPath = path.join(__dirname, folderPath);
    console.log("The dist path is:",distFolderPath);
    try{
      fs.readdir(distFolderPath, (err, files) => {     
        if(!files || files.length === 0) {
          result = false;
          console.log(`provided folder '${distFolderPath}' is empty or does not exist.`);
          reject({code:400, error:"No file present in the directory"});
        }
        if(err){ throw err;}
        for (const fileName of files) {
      
          const filePath = path.join(distFolderPath, fileName);
          
          if (fs.lstatSync(filePath).isDirectory()) {
            continue;
          }
          fs.readFile(filePath, async (error, fileContent) => {
            if (error) {
              result = false;
               throw error; }
      
            await s3.putObject({
              Bucket: config.s3BucketName,
              Key: fileName,
              Body: fileContent
            }, function(err,data) {
              if(err)
              {
                  reject({code:400, error:"Error ocurred"})
              }
              else{
                deleteFile.deleteLocalFile(filePath);
                console.log("Path is:", filePath);
                console.log(`Successfully uploaded '${fileName}'!`);
              }
            });
      
          });
        }
      });
    }catch(error){
      console.log("Error")
      reject({code:400, error:"File upload not successful"});
    }
     
      if(result)
      {
        resolve({code:200,message:"Upload Successfull"});
      }
      else{
        reject({code:400, error:"Not uploaded"});
      }
    })
   
}

