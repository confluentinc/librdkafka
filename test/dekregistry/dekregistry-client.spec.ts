import { DekRegistryClient, Dek, Kek } from "../../dekregistry/dekregistry-client";
import { RestService } from "../../schemaregistry/rest-service";
import { AxiosResponse } from 'axios';
import { beforeEach, afterEach, describe, expect, it, jest } from '@jest/globals';
import { TEST_KEK, TEST_KEK_2, TEST_KEK_NAME, TEST_KEK_NAME_2, TEST_KMS_TYPE, TEST_KMS_KEY_ID, 
  TEST_KMS_PROPS, TEST_DOC, TEST_DEK, TEST_DEK_2, TEST_ALGORITHM, 
  TEST_ENCRYPTED_KEY_MATERIAL, TEST_SUBJECT, TEST_VERSION, 
  TEST_DEK_LATEST} from "./test-constants";
import { mockClientConfig } from "../schemaregistry/test-constants";

jest.mock('../../schemaregistry/rest-service');


let client: DekRegistryClient;
let restService: jest.Mocked<RestService>;

describe('DekRegistryClient', () => {

  beforeEach(() => {
    restService = new RestService(mockClientConfig.createAxiosDefaults, mockClientConfig.baseURLs) as jest.Mocked<RestService>;
    client = new DekRegistryClient(mockClientConfig);
    (client as any).restService = restService;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should register kek when registerKek is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: TEST_KEK } as AxiosResponse);
    const response: Kek = await client.registerKek(
      TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);

    expect(response).toEqual(TEST_KEK);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return kek from cache when registerKek is called with same kek name', async () => {
    restService.handleRequest.mockResolvedValue({ data: TEST_KEK } as AxiosResponse);
    await client.registerKek(TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);
    restService.handleRequest.mockResolvedValue({ data: TEST_KEK_2 } as AxiosResponse);
    await client.registerKek(TEST_KEK_NAME_2, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);
    
    const response: Kek = await client.registerKek(
      TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);
    const response2: Kek = await client.registerKek(
      TEST_KEK_NAME_2, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);

    expect(response).toEqual(TEST_KEK);
    expect(response2).toEqual(TEST_KEK_2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return kek from cache when getKek is called with same kek name', async () => {
    restService.handleRequest.mockResolvedValue({ data: TEST_KEK } as AxiosResponse);
    await client.registerKek(TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);
    const response: Kek = await client.getKek(TEST_KEK_NAME);

    expect(response).toEqual(TEST_KEK);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should register dek when registerDek is called', async () => {
    restService.handleRequest.mockResolvedValue({ data: TEST_DEK } as AxiosResponse);
    const response: Dek = await client.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
    expect(response).toEqual(TEST_DEK);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return dek from cache when registerDek is called with same kek name, subject, algorithm, and version', async () => {
    restService.handleRequest.mockResolvedValue({ data: TEST_DEK } as AxiosResponse);
    await client.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
    restService.handleRequest.mockResolvedValue({ data: TEST_DEK_2 } as AxiosResponse);
    await client.registerDek(TEST_KEK_NAME_2, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
    
    const response: Dek = await client.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
    const response2: Dek = await client.registerDek(TEST_KEK_NAME_2, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);

    expect(response).toEqual(TEST_DEK);
    expect(response2).toEqual(TEST_DEK_2);
    expect(restService.handleRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return dek from cache when getDek is called with same kek name, subject, algorithm, and version', async () => {
    restService.handleRequest.mockResolvedValue({ data: TEST_DEK } as AxiosResponse);
    await client.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
    const response: Dek = await client.getDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_VERSION);

    expect(response).toEqual(TEST_DEK);
    expect(restService.handleRequest).toHaveBeenCalledTimes(1);
  });

  it('Should delete dek with version -1 when registerDek is called', async () => {
  restService.handleRequest.mockResolvedValue({ data: TEST_DEK_LATEST } as AxiosResponse);
  const getDekResponse: Dek = await client.getDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, -1);
  expect(getDekResponse).toEqual(TEST_DEK_LATEST);
  expect(await client.checkLatestDekInCache(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM)).toBe(true);

  restService.handleRequest.mockResolvedValue({ data: TEST_DEK } as AxiosResponse);
  await client.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
  const getDekResponse2: Dek = await client.getDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM);
  
  expect(getDekResponse2).toEqual(TEST_DEK);
  expect(await client.checkLatestDekInCache(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM)).toBe(false);
  });
});