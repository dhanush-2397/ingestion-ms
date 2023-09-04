import { Test, TestingModule } from '@nestjs/testing';
import { NvskApiService } from './nvsk-api.service';

describe('NvskApiService', () => {
  let service: NvskApiService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [NvskApiService],
    }).compile();

    service = module.get<NvskApiService>(NvskApiService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
