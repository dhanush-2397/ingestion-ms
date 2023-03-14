import {HttpCustomService} from './services/HttpCustomService';
import {HttpModule} from '@nestjs/axios';
import {Module} from '@nestjs/common';
import {DatabaseModule} from 'src/database/database.module';
import {IngestionController} from './controller/ingestion.controller';
import {DatasetService} from './services/dataset/dataset.service';
import {DimensionService} from './services/dimension/dimension.service';
import {EventService} from './services/event/event.service';
import {GenericFunction} from './services/generic-function';
import {CsvImportService} from "./services/csvImport/csvImport.service";
import {FileStatusService} from './services/file-status/file-status.service';
import {UpdateFileStatusService} from "./services/update-file-status/update-file-status.service";
import {CsvToJsonService} from './services/csv-to-json/csv-to-json.service';
import {DataEmissionService} from "./services/data-emission/data-emission.service";
import {V4DataEmissionService} from "./services/v4-data-emission/v4-data-emission.service";
import {AzureUpload} from "./services/azure-upload";

@Module({
    controllers: [IngestionController],
    providers: [DatasetService, DimensionService, EventService, GenericFunction, HttpCustomService, CsvImportService, FileStatusService,
        UpdateFileStatusService, CsvToJsonService, DataEmissionService, V4DataEmissionService,AzureUpload],
    imports: [DatabaseModule, HttpModule]
})
export class IngestionModule {
}
