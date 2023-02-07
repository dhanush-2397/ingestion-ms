import {ApiProperty} from "@nestjs/swagger";

export class Dimension {
    @ApiProperty()
    dimension_name: string
    @ApiProperty()
    dimension: object[];
    @ApiProperty()
    file_tracker_pid: number;
}

export class IEvent {
    @ApiProperty()
    event_name: string
    @ApiProperty()
    event: object[];
    @ApiProperty()
    file_tracker_pid: number;
}

export class Dataset {
    @ApiProperty()
    dataset_name: string
    @ApiProperty()
    dataset: any;
    @ApiProperty()
    file_tracker_pid: number;
}

export class Pipeline {
    @ApiProperty()
    pipeline_name: string
}


export interface Result {
    code: number,
    message?: string,
    error?: string
}

export class FileStatus {
    @ApiProperty()
    filename?: string
    @ApiProperty()
    ingestion_type?: string
    @ApiProperty()
    ingestion_name?: string
}

export class CSVBody {
    @ApiProperty({type: 'string', format: 'binary', required: true})
    file: Express.Multer.File;
    @ApiProperty({type: 'string'})
    ingestion_type: string;
    @ApiProperty({type: 'string'})
    ingestion_name: string;
}

export class FileStatusInterface {
    @ApiProperty()
    file_name: string;    
    @ApiProperty()
    ingestion_type: string;
    @ApiProperty()
    ingestion_name: string;
    @ApiProperty()
    status: string;
}