import { SchemaRegistryClient, SchemaInfo, ClientConfig } from '@confluentinc/schemaregistry';
import { v4 as uuidv4 } from 'uuid';
import { CreateAxiosDefaults } from 'axios';
import { localAuthCredentials } from './constants';

async function localDemo() {
  const createAxiosDefaults: CreateAxiosDefaults = {
    timeout: 10000
  };

  const clientConfig: ClientConfig = {
    baseURLs: ['http://localhost:8081'],
    createAxiosDefaults: createAxiosDefaults,
    cacheCapacity: 512,
    cacheLatestTtlSecs: 60,
    basicAuthCredentials: localAuthCredentials,
  };

  const schemaString: string = JSON.stringify({
    type: 'record',
    name: 'User',
    fields: [
      { name: 'name', type: 'string' },
      { name: 'age', type: 'int' },
    ],
  });

  const schemaInfo: SchemaInfo = {
    schemaType: 'AVRO',
    schema: schemaString,
  };

  const schemaRegistryClient = new SchemaRegistryClient(clientConfig);

  console.log("Current Subjects: ", await schemaRegistryClient.getAllSubjects());

  const subject1 = `subject-${uuidv4()}`;
  const subject2 = `subject-${uuidv4()}`;
  console.log("subject1: ", subject1);
  console.log("subject2: ", subject2);

  await schemaRegistryClient.register(subject1, schemaInfo);
  await schemaRegistryClient.register(subject2, schemaInfo);

  console.log("Subjects After Registering: ", await schemaRegistryClient.getAllSubjects());
}

localDemo();