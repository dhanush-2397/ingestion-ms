import { Injectable } from "@nestjs/common";
import { HttpCustomService } from "../HttpCustomService";
import { RawDataResponse } from "../../interfaces/Ingestion-data";
import { UploadService } from "../file-uploader-service";

@Injectable()
export class RawDataImportService {
  constructor(
    private http: HttpCustomService,
    private uploadService: UploadService
  ) {}
  async readFiles(inputBody): Promise<RawDataResponse> {
    return new Promise(async (resolve, reject) => {
      try {
        const result = inputBody.program_names.map(async (program_name) => {
          const obj = {
            program_name: program_name,
            urls: [],
          };
          const key = `emission/`;

          await this.uploadService
            .getFolderNames(key)
            .then(async (objects: any) => {
              const parsedDates = objects.map((dateStr) => new Date(dateStr));
              var latestDate: any = new Date(Math.max(...parsedDates));
              latestDate = `${latestDate.getDate()}-${formatMonth(latestDate.getMonth())}-${latestDate.getFullYear()}`;

              function formatMonth(monthIndex) {
                const monthNames = [
                  "Jan",
                  "Feb",
                  "Mar",
                  "Apr",
                  "May",
                  "Jun",
                  "Jul",
                  "Aug",
                  "Sep",
                  "Oct",
                  "Nov",
                  "Dec",
                ];
                return monthNames[monthIndex];
              }
              await this.uploadService
                .getFolderObjects(`${key}${latestDate}`)
                .then(async (objectNames: any) => {
                  objectNames = objectNames.filter((objectName) =>
                    objectName.includes(program_name)
                  );
                  const promises = objectNames.map(async (objectKey) => {
                    const url = await this.uploadService.fileDownloaderUrl(
                      objectKey
                    );
                    return url;
                  });
                  // Wait for all promises to resolve
                  const a = await Promise.all(promises);
                  obj.urls = a;
                });
            });
          return obj;
        });
        const data = await Promise.all(result);
        resolve({ code: 200, data: data });
      } catch (error) {
        reject({ code: "", error: "" });
      }
    });
  }
}
