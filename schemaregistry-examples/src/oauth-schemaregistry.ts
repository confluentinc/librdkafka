import { SchemaRegistryClient, BearerAuthCredentials, ClientConfig } from '@confluentinc/schemaregistry';
import { CreateAxiosDefaults } from 'axios';
import {
  issuerEndpointUrl, oauthClientId, oauthClientSecret, scope,
  identityPoolId, schemaRegistryLogicalCluster, baseUrl
} from './constants';

async function oauthSchemaRegistry() {

  const bearerAuthCredentials: BearerAuthCredentials = {
    credentialsSource: 'OAUTHBEARER',
    issuerEndpointUrl: issuerEndpointUrl,
    clientId: oauthClientId,
    clientSecret: oauthClientSecret,
    scope: scope,
    identityPoolId: identityPoolId,
    logicalCluster: schemaRegistryLogicalCluster
  }

  const createAxiosDefaults: CreateAxiosDefaults = {
    timeout: 10000
  };

  const clientConfig: ClientConfig = {
    baseURLs: [baseUrl],
    createAxiosDefaults: createAxiosDefaults,
    cacheCapacity: 512,
    cacheLatestTtlSecs: 60,
    bearerAuthCredentials: bearerAuthCredentials
  };

  const schemaRegistryClient = new SchemaRegistryClient(clientConfig);

  console.log("Current Subjects:", await schemaRegistryClient.getAllSubjects());
  console.log("Current Config:", await schemaRegistryClient.getDefaultConfig());
  console.log("Current Compatibility", await schemaRegistryClient.getDefaultCompatibility());
}

oauthSchemaRegistry();