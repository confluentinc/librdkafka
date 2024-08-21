import { CreateAxiosDefaults } from 'axios';
import { ClientConfig } from '../../schemaregistry/rest-service';

const baseUrls = ['http://localhost:8081'];

const mockBaseUrls = ['http://mocked-url'];

const createAxiosDefaults: CreateAxiosDefaults = {
  headers: {
    'Content-Type': 'application/vnd.schemaregistry.v1+json',
  },
  auth: {
    username: 'RBACAllowedUser-lsrc1',
    password: 'nohash',
  },
  timeout: 10000
};

const clientConfig: ClientConfig = {
  baseURLs: baseUrls,
  createAxiosDefaults: createAxiosDefaults,
  isForward: false,
  cacheCapacity: 512,
  cacheLatestTtlSecs: 60,
};

const mockClientConfig: ClientConfig = {
  baseURLs: mockBaseUrls,
  createAxiosDefaults: createAxiosDefaults,
  isForward: false,
  cacheCapacity: 512,
  cacheLatestTtlSecs: 60,
};

export { clientConfig, mockClientConfig };
