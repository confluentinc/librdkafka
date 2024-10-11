import { BasicAuthCredentials } from '@confluentinc/schemaregistry';

const issuerEndpointUrl = '<your-issuer-endpoint-url>'; // e.g. 'https://dev-123456.okta.com/oauth2/default/v1/token';
const oauthClientId = '<your-client-id>';
const oauthClientSecret = '<your-client-secret>';
const scope = '<your-scope>'; // e.g. 'schemaregistry';
const identityPoolId = '<your-pool>'; // e.g. pool-Gx30
const kafkaLogicalCluster = '<your-logical-cluster>'; // e.g. lkc-12345
const schemaRegistryLogicalCluster = '<your-logical-cluster>'; //e.g. lsrc-a6m5op
const baseUrl = '<your-schema-registry-url>'; // e.g. 'https://psrc-3amt5nj.us-east-1.aws.confluent.cloud'
const clusterBootstrapUrl = '<your-cluster-bootstrap-url>'; // e.g. "pkc-p34xa.us-east-1.aws.confluent.cloud:9092"
const clusterApiKey = '<your-cluster-api-key>';
const clusterApiSecret = '<your-cluster-api-secret>';

const localAuthCredentials: BasicAuthCredentials = {
  credentialsSource: 'USER_INFO',
  userInfo: 'RBACAllowedUser-lsrc1:nohash',
};

const basicAuthCredentials: BasicAuthCredentials = {
  credentialsSource: 'USER_INFO',
  userInfo: '<client-id>:<client-secret>',
};

export {
  issuerEndpointUrl, oauthClientId, oauthClientSecret, scope, identityPoolId, kafkaLogicalCluster, schemaRegistryLogicalCluster, 
  baseUrl, clusterBootstrapUrl, clusterApiKey, clusterApiSecret, basicAuthCredentials, localAuthCredentials
};