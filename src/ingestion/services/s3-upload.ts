const AWS = require("aws-sdk"); // from AWS SDK
const fs = require("fs"); // from node.js
const path = require("path"); // from node.js
import { resolve } from "path";
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
export async function uploadFilestoS3(): Promise<Result> {
    return new Promise(async (resolve, reject)=>{
    let result:boolean;
    const distFolderPath = path.join(__dirname, config.folderPath);
    console.log("The dist path is:",distFolderPath);
     fs.readdir(distFolderPath, (err, files) => {
        if(!files || files.length === 0) {
          console.log(`provided folder '${distFolderPath}' is empty or does not exist.`);
          console.log('Make sure your project was compiled!');
          reject({code:400, error:"No file present in the directory"});
        }
        if(err){ throw err;}
        let numberOfFiles = files.length;
        let uploadFileCounter = 0;
        for (const fileName of files) {
      
          const filePath = path.join(distFolderPath, fileName);
          
          if (fs.lstatSync(filePath).isDirectory()) {
            continue;
          }
          fs.readFile(filePath, async (error, fileContent) => {
            if (error) { throw error; }
      
            await s3.putObject({
              Bucket: config.s3BucketName,
              Key: fileName,
              Body: fileContent
            }, function(err,data) {
              if(err)
              {
                  console.log("error occured",err);
                  reject({code:400, error:"Error ocurred"})
              }
              else{
                uploadFileCounter = uploadFileCounter + 1
                deleteFile.deleteLocalFile(filePath);
                console.log("Path is:", filePath);
                console.log(`Successfully uploaded '${fileName}'!`);
              }
            });
      
          });
        }
      });
      resolve({code:200,message:"Upload Successfull"});
    })
    
       
}

