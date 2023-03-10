require("dotenv").config();
import {lookup} from "mime-types";
import {Client} from "minio"

const minioClient = new Client({
    endPoint: process.env.END_POINT,
    port: +process.env.S3_PORT,
    useSSL: false,
    accessKey: process.env.ACCESS_KEY,
    secretKey: process.env.SECRET_KEY
});

minioClient.listBuckets(function (e, buckets) {
    if (e) return console.log(e);
    console.log('buckets :', buckets)
});

export async function uploadToS3(bucketName, path, fileName) {
    let metaData = {
        "Content-Type": lookup(fileName),
    };

    minioClient.fPutObject(
        bucketName,
        fileName,
        path,
        metaData,
        function (err, objInfo) {
            if (err) {
                return console.log(err); // err should be null
            }
            console.log("Success", objInfo);
        }
    );
}
