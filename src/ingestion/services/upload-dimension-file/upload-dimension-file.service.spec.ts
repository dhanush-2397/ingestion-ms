import { Test, TestingModule } from '@nestjs/testing';
import { UploadDimensionFileService } from './upload-dimension-file.service';

describe('UploadDimensionFileService', () => {
  let service: UploadDimensionFileService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [UploadDimensionFileService],
    }).compile();

    service = module.get<UploadDimensionFileService>(UploadDimensionFileService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
