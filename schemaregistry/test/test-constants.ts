import { CreateAxiosDefaults } from 'axios';
import { ClientConfig, BasicAuthCredentials } from '../rest-service';

const baseUrls = ['http://localhost:8081'];

const mockBaseUrls = ['http://mocked-url'];

const createAxiosDefaults: CreateAxiosDefaults = {
  timeout: 10000
};

const basicAuthCredentials: BasicAuthCredentials = {
  credentialsSource: 'USER_INFO',
  userInfo: 'RBACAllowedUser-lsrc1:nohash',
};

const clientConfig: ClientConfig = {
  baseURLs: baseUrls,
  createAxiosDefaults: createAxiosDefaults,
  isForward: false,
  cacheCapacity: 512,
  cacheLatestTtlSecs: 60,
  basicAuthCredentials: basicAuthCredentials,
};

const mockClientConfig: ClientConfig = {
  baseURLs: mockBaseUrls,
  createAxiosDefaults: createAxiosDefaults,
  isForward: false,
  cacheCapacity: 512,
  cacheLatestTtlSecs: 60,
  basicAuthCredentials: basicAuthCredentials
};

const mockTtlClientConfig: ClientConfig = {
  baseURLs: mockBaseUrls,
  createAxiosDefaults: createAxiosDefaults,
  isForward: false,
  cacheCapacity: 512,
  cacheLatestTtlSecs: 1,
  basicAuthCredentials: basicAuthCredentials
};

const maxRetries = 2;
const retriesWaitMs = 100;
const retriesMaxWaitMs = 1000;

export { clientConfig, mockClientConfig, mockTtlClientConfig, maxRetries, retriesWaitMs, retriesMaxWaitMs };
