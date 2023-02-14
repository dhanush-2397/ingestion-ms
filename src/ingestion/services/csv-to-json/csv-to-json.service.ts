import { Injectable } from '@nestjs/common';
import * as csv from 'csvtojson';
import * as fs from 'fs';

@Injectable()
export class CsvToJsonService {

    async convertCsvToJson(file) {
        try {
            if (fs.existsSync('./main_metric.json')) {
                fs.unlinkSync('./main_metric.json');
            }
            const data = await csv().fromString(file.buffer.toString());
            const json = JSON.stringify(data);
            fs.writeFileSync('./main_metric.json', json, 'utf-8');
            return {
                code: 200,
                message: 'File converted successfully',
                response: data
            };
        } catch (error) {
            console.log('csvToJson', error.message);
            return {
                "code": 400, "error": error.message
            }
        }
    }
}

