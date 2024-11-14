import {
  SchemaRegistryClient,
  Metadata,
  Compatibility,
  SchemaInfo,
  SchemaMetadata,
  ServerConfig
} from '../../schemaregistry/schemaregistry-client';
import { RestService } from '../../schemaregistry/rest-service';
import { AxiosResponse } from 'axios';
import stringify from "json-stringify-deterministic";
import { beforeEach, afterEach, describe, expect, it, jest } from '@jest/globals';
import { mockClientConfig, mockTtlClientConfig } from '../../test/schemaregistry/test-constants';

jest.mock('../../schemaregistry/rest-service');

let client: SchemaRegistryClient;
let restService: jest.Mocked<RestService>;
const mockSubject = 'mock-subject';
const mockSubject2 = 'mock-subject2';
const schemaString = stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' }
  ]
});
const schemaString2 = stringify({
  type: 'record',
  name: 'User2',
  fields: [
    { name: 'name2', type: 'string' },
    { name: 'age2', type: 'int' }
  ]
});
const metadata: Metadata = {
  properties: {
    owner: 'Alice Bob',
    email: 'Alice@bob.com',
  }
};
const metadata2: Metadata = {
  properties: {
    owner: 'Alice Bob2',
    email: 'Alice@bob2.com',
  }
};
const metadataKeyValue = {
  'owner': 'Alice Bob',
  'email': 'Alice@bob.com',
}

const metadataKeyValue2 = {
  'owner': 'Alice Bob2',
  'email': 'Alice@bob2.com'
};
const schemaInfo = {
  schema: schemaString,
  schemaType: 'AVRO',
};
const schemaInfo2 = {
  schema: schemaString,
  schemaType: 'AVRO',
};
const schemaInfoMetadata = {
  schema: schemaString,
  schemaType: 'AVRO',
  metadata: metadata,
};
const schemaInfoMetadata2 = {
  schema: schemaString,
  schemaType: 'AVRO',
  metadata: metadata2,
};
const subjects: string[] = [mockSubject, mockSubject2];
const versions: number[] = [1, 2, 3];

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('SchemaRegistryClient-Register', () => {

  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return id when Register is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);

    const response: number = await client.register(mockSubject, schemaInfo);

    expect(response).toEqual(1);

    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return from cache when Register is called twice', async () => {
    restService.handleRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);

    const response: number = await client.register(mockSubject, schemaInfo);
    expect(response).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: { id: 2 } } as AxiosResponse);

    const response2: number = await client.register(mockSubject2, schemaInfo2);
    expect(response2).toEqual(2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    //Try to create same objects again

    const cachedResponse: number = await client.register(mockSubject, schemaInfo);
    expect(cachedResponse).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: number = await client.register(mockSubject2, schemaInfo2);
    expect(cachedResponse2).toEqual(2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return id, version, metadata, and schema when RegisterFullResponse is called', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaMetadata = await client.registerFullResponse(mockSubject, schemaInfoMetadata);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return id, version, metadata, and schema from cache when RegisterFullResponse is called twice', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const expectedResponse2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaMetadata = await client.registerFullResponse(mockSubject, schemaInfoMetadata);
    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: expectedResponse2 } as AxiosResponse);

    const response2: SchemaMetadata = await client.registerFullResponse(mockSubject2, schemaInfoMetadata2);
    expect(response2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse: SchemaMetadata = await client.registerFullResponse(mockSubject, schemaInfoMetadata);
    expect(cachedResponse).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: SchemaMetadata = await client.registerFullResponse(mockSubject2, schemaInfoMetadata2);
    expect(cachedResponse2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });
});

describe('SchemaRegistryClient-Get-ID', () => {
  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return id when GetId is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);

    const response: number = await client.getId(mockSubject, schemaInfo);

    expect(response).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return id from cache when GetId is called twice', async () => {
    restService.handleRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);

    const response: number = await client.getId(mockSubject, schemaInfo);
    expect(response).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: { id: 2 } } as AxiosResponse);

    const response2: number = await client.getId(mockSubject2, schemaInfo2);
    expect(response2).toEqual(2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse: number = await client.getId(mockSubject, schemaInfo);
    expect(cachedResponse).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: number = await client.getId(mockSubject2, schemaInfo2);
    expect(cachedResponse2).toEqual(2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return SchemaInfo when GetBySubjectAndId is called', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaInfo = await client.getBySubjectAndId(mockSubject, 1);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return SchemaInfo from cache when GetBySubjectAndId is called twice', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const expectedResponse2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaInfo = await client.getBySubjectAndId(mockSubject, 1);
    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: expectedResponse2 } as AxiosResponse);

    const response2: SchemaInfo = await client.getBySubjectAndId(mockSubject2, 2);
    expect(response2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse: SchemaInfo = await client.getBySubjectAndId(mockSubject, 1);
    expect(cachedResponse).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: SchemaInfo = await client.getBySubjectAndId(mockSubject2, 2);
    expect(cachedResponse2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });
});

describe('SchemaRegistryClient-Get-Schema-Metadata', () => {
  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return latest schema with metadata when GetLatestWithMetadata is called', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaMetadata = await client.getLatestWithMetadata(mockSubject, metadataKeyValue);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return latest schema with metadata from cache when GetLatestWithMetadata is called twice', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const expectedResponse2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaMetadata = await client.getLatestWithMetadata(mockSubject, metadataKeyValue);
    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: expectedResponse2 } as AxiosResponse);

    const response2: SchemaMetadata = await client.getLatestWithMetadata(mockSubject2, metadataKeyValue2);
    expect(response2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse: SchemaMetadata = await client.getLatestWithMetadata(mockSubject, metadataKeyValue);
    expect(cachedResponse).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: SchemaMetadata = await client.getLatestWithMetadata(mockSubject2, metadataKeyValue2);
    expect(cachedResponse2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return SchemaMetadata when GetSchemaMetadata is called', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaMetadata = await client.getSchemaMetadata(mockSubject, 1, true);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return SchemaMetadata from cache when GetSchemaMetadata is called twice', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const expectedResponse2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: SchemaMetadata = await client.getSchemaMetadata(mockSubject, 1, true);
    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: expectedResponse2 } as AxiosResponse);

    const response2: SchemaMetadata = await client.getSchemaMetadata(mockSubject2, 2, false);
    expect(response2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse: SchemaMetadata = await client.getSchemaMetadata(mockSubject, 1, true);
    expect(cachedResponse).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: SchemaMetadata = await client.getSchemaMetadata(mockSubject2, 2, false);
    expect(cachedResponse2).toMatchObject(expectedResponse2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });
});

describe('SchemaRegistryClient-Subjects', () => {
  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return all subjects when GetAllSubjects is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: subjects } as AxiosResponse);

    const response: string[] = await client.getAllSubjects();

    expect(response).toEqual(subjects);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return all versions when GetAllVersions is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: versions } as AxiosResponse);

    const response: number[] = await client.getAllVersions(mockSubject);

    expect(response).toEqual(versions);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return version when GetVersion is called', async () => {
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };
    restService.handleRequest.mockResolvedValue({ data: { version: 1 } } as AxiosResponse);

    const response: number = await client.getVersion(mockSubject, schemaInfo, true);

    expect(response).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return version from cache when GetVersion is called twice', async () => {
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };
    const schemaInfo2 = {
      schema: schemaString2,
      schemaType: 'AVRO',
    };

    restService.handleRequest.mockResolvedValue({ data: { version: 1 } } as AxiosResponse);

    const response: number = await client.getVersion(mockSubject, schemaInfo, true);
    expect(response).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);

    restService.handleRequest.mockResolvedValue({ data: { version: 2 } } as AxiosResponse);

    const response2: number = await client.getVersion(mockSubject2, schemaInfo2, false);
    expect(response2).toEqual(2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse: number = await client.getVersion(mockSubject, schemaInfo, true);
    expect(cachedResponse).toEqual(1);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);

    const cachedResponse2: number = await client.getVersion(mockSubject2, schemaInfo2, false);
    expect(cachedResponse2).toEqual(2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });

  it('Should delete subject from all caches and registry when deleteSubject is called', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    await client.addToInfoToSchemaCache(mockSubject, schemaInfo, expectedResponse);
    await client.addToSchemaToVersionCache(mockSubject, schemaInfo, 1);
    await client.addToVersionToSchemaCache(mockSubject, 1, expectedResponse);
    await client.addToIdToSchemaInfoCache(mockSubject, 1, schemaInfo);

    restService.handleRequest.mockResolvedValue({ data: [1] } as AxiosResponse);

    const response: number[] = await client.deleteSubject(mockSubject);

    expect(await client.getInfoToSchemaCacheSize()).toEqual(0);
    expect(await client.getSchemaToVersionCacheSize()).toEqual(0);
    expect(await client.getVersionToSchemaCacheSize()).toEqual(0);
    expect(await client.getIdToSchemaInfoCacheSize()).toEqual(0);

    expect(response).toEqual([1]);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should delete subject version from all caches and registry when deleteSubjectVersion is called', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    await client.addToInfoToSchemaCache(mockSubject, schemaInfo, expectedResponse);
    await client.addToSchemaToVersionCache(mockSubject, schemaInfo, 1);
    await client.addToVersionToSchemaCache(mockSubject, 1, expectedResponse);
    await client.addToIdToSchemaInfoCache(mockSubject, 1, schemaInfo);

    restService.handleRequest.mockResolvedValue({ data: [1] } as AxiosResponse);

    const response: number = await client.deleteSubjectVersion(mockSubject, 1);

    expect(await client.getVersionToSchemaCacheSize()).toEqual(0);
    expect(await client.getInfoToSchemaCacheSize()).toEqual(0);
    expect(await client.getSchemaToVersionCacheSize()).toEqual(0);
    expect(await client.getIdToSchemaInfoCacheSize()).toEqual(0);

    expect(response).toEqual([1]);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });
});

describe('SchemaRegistryClient-Compatibility', () => {
  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return compatibility level when GetCompatibility is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: { compatibilityLevel: "BACKWARD" } } as AxiosResponse);

    const response: Compatibility = await client.getCompatibility(mockSubject);

    expect(response).toEqual('BACKWARD');
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should update compatibility level when updateCompatibility is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: { compatibility: 'BACKWARD' } } as AxiosResponse);

    const response: Compatibility = await client.updateCompatibility(mockSubject, Compatibility.BACKWARD);

    expect(response).toEqual(Compatibility.BACKWARD);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return Compatibility when getDefaultCompatibility is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: { compatibilityLevel: 'BACKWARD' } } as AxiosResponse);

    const response: Compatibility = await client.getDefaultCompatibility();

    expect(response).toEqual(Compatibility.BACKWARD);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should update default compatibility level when updateDefaultCompatibility is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: { compatibility: 'BACKWARD' } } as AxiosResponse);

    const response: Compatibility = await client.updateDefaultCompatibility(Compatibility.BACKWARD);

    expect(response).toEqual(Compatibility.BACKWARD);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });
});

describe('SchemaRegistryClient-Config', () => {
  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return config when getConfig is called', async () => {
    const expectedResponse = {
      compatibilityLevel: 'BACKWARD',
      alias: 'test-config',
      normalize: true,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: ServerConfig = await client.getConfig(mockSubject);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should update config when updateConfig is called', async () => {
    const request = {
      compatibility: Compatibility.BACKWARD,
      alias: 'test-config',
      normalize: true,
    };
    const expectedResponse = {
      compatibilityLevel: 'BACKWARD',
      alias: 'test-config',
      normalize: true,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: ServerConfig = await client.updateConfig(mockSubject, request);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return config when getDefaultConfig is called', async () => {
    const expectedResponse = {
      compatibilityLevel: 'BACKWARD',
      alias: 'test-config',
      normalize: true,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: ServerConfig = await client.getDefaultConfig();

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should update default config when updateDefaultConfig is called', async () => {
    const request = {
      compatibility: Compatibility.BACKWARD,
      alias: 'test-config',
      normalize: true,
    };
    const expectedResponse = {
      compatibilityLevel: 'BACKWARD',
      alias: 'test-config',
      normalize: true,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    const response: ServerConfig = await client.updateDefaultConfig(request);

    expect(response).toMatchObject(expectedResponse);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });
});

describe('SchemaRegistryClient-Cache', () => {
  beforeEach(() => {
    restService = new RestService(mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(mockTtlClientConfig);
    (client as any).restService = restService;
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should delete cached item after expiry', async () => {
    const expectedResponse = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.handleRequest.mockResolvedValue({ data: expectedResponse } as AxiosResponse);

    await client.register(mockSubject, schemaInfo);

    await sleep(2000);

    await client.register(mockSubject, schemaInfo);

    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });
});
