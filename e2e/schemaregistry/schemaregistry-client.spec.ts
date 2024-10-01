import {
  Compatibility,
  SchemaRegistryClient,
  ServerConfig,
  SchemaInfo,
  SchemaMetadata,
  Metadata
} from '../../schemaregistry/schemaregistry-client';
import { beforeEach, describe, expect, it } from '@jest/globals';
import { clientConfig } from '../../test/schemaregistry/test-constants';
import { v4 } from 'uuid';

/* eslint-disable @typescript-eslint/no-non-null-asserted-optional-chain */

let schemaRegistryClient: SchemaRegistryClient;
const testSubject = 'integ-test-subject';
const testServerConfigSubject = 'integ-test-server-config-subject';

const schemaString: string = JSON.stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' },
  ],
});

const metadata: Metadata = {
  properties: {
    owner: 'Bob Jones',
    email: 'bob@acme.com',
  },
};

const schemaInfo: SchemaInfo = {
  schema: schemaString,
  metadata: metadata,
};

const backwardCompatibleSchemaString: string = JSON.stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' },
    { name: 'email', type: 'string', default: "" },
  ],
});

const backwardCompatibleMetadata: Metadata = {
  properties: {
    owner: 'Bob Jones2',
    email: 'bob@acme.com',
  },
};

const backwardCompatibleSchemaInfo: SchemaInfo = {
  schema: backwardCompatibleSchemaString,
  schemaType: 'AVRO',
  metadata: backwardCompatibleMetadata,
};

describe('SchemaRegistryClient Integration Test', () => {

  beforeEach(async () => {
    schemaRegistryClient = new SchemaRegistryClient(clientConfig);
    const subjects: string[] = await schemaRegistryClient.getAllSubjects();

    if (subjects && subjects.includes(testSubject)) {
      await schemaRegistryClient.deleteSubject(testSubject);
      await schemaRegistryClient.deleteSubject(testSubject, true);
    }

    if (subjects && subjects.includes(testServerConfigSubject)) {
      await schemaRegistryClient.deleteSubject(testServerConfigSubject);
      await schemaRegistryClient.deleteSubject(testServerConfigSubject, true);
    }
  });

  it("Should return RestError when retrieving non-existent schema", async () => {
    await expect(schemaRegistryClient.getLatestSchemaMetadata(v4())).rejects.toThrow();
  });

  it('Should register, retrieve, and delete a schema', async () => {
    // Register a schema
    const registerResponse:  SchemaMetadata = await schemaRegistryClient.registerFullResponse(testSubject, schemaInfo);
    expect(registerResponse).toBeDefined();

    const schemaId = registerResponse?.id!;
    const version = registerResponse?.version!;

    const getSchemaResponse: SchemaInfo = await schemaRegistryClient.getBySubjectAndId(testSubject, schemaId);
    expect(getSchemaResponse).toEqual(schemaInfo);

    const getIdResponse: number = await schemaRegistryClient.getId(testSubject, schemaInfo);
    expect(getIdResponse).toEqual(schemaId);

    // Delete the schema
    const deleteSubjectResponse: number = await schemaRegistryClient.deleteSubjectVersion(testSubject, version);
    expect(deleteSubjectResponse).toEqual(version);

    const permanentDeleteSubjectResponse: number = await schemaRegistryClient.deleteSubjectVersion(testSubject, version, true);
    expect(permanentDeleteSubjectResponse).toEqual(version);
  });

  it('Should get all versions and a specific version of a schema', async () => {
    // Register a schema
    const registerResponse:  SchemaMetadata = await schemaRegistryClient.registerFullResponse(testSubject, schemaInfo);
    expect(registerResponse).toBeDefined();

    const version = registerResponse?.version!;

    const getVersionResponse: number = await schemaRegistryClient.getVersion(testSubject, schemaInfo);
    expect(getVersionResponse).toEqual(version);

    const allVersionsResponse: number[] = await schemaRegistryClient.getAllVersions(testSubject);
    expect(allVersionsResponse).toEqual([version]);
  });

  it('Should get schema metadata', async () => {
    // Register a schema
    const registerResponse:  SchemaMetadata = await schemaRegistryClient.registerFullResponse(testSubject, schemaInfo);
    expect(registerResponse).toBeDefined();

    const schemaVersion: number = registerResponse?.version!;

    const registerResponse2:  SchemaMetadata = await schemaRegistryClient.registerFullResponse(testSubject, backwardCompatibleSchemaInfo);
    expect(registerResponse2).toBeDefined();

    const schemaMetadata: SchemaMetadata = {
      id: registerResponse?.id!,
      version: schemaVersion,
      schema: schemaInfo.schema,
      subject: testSubject,
      metadata: metadata,
    };

    const schemaMetadata2: SchemaMetadata = {
      id: registerResponse2?.id!,
      version: registerResponse2?.version!,
      schema: backwardCompatibleSchemaInfo.schema,
      subject: testSubject,
      metadata: backwardCompatibleMetadata,
    };

    const getLatestMetadataResponse:  SchemaMetadata = await schemaRegistryClient.getLatestSchemaMetadata(testSubject);
    expect(schemaMetadata2).toEqual(getLatestMetadataResponse);

    const getMetadataResponse:  SchemaMetadata = await schemaRegistryClient.getSchemaMetadata(testSubject, schemaVersion);
    expect(schemaMetadata).toEqual(getMetadataResponse);

    const keyValueMetadata: { [key: string]: string } = {
      'owner': 'Bob Jones',
      'email': 'bob@acme.com'
    }

    const getLatestWithMetadataResponse:  SchemaMetadata = await schemaRegistryClient.getLatestWithMetadata(testSubject, keyValueMetadata);
    expect(schemaMetadata).toEqual(getLatestWithMetadataResponse);
  });

  it('Should test compatibility for a version and subject, getting and updating', async () => {
    const registerResponse:  SchemaMetadata = await schemaRegistryClient.registerFullResponse(testSubject, schemaInfo);
    expect(registerResponse).toBeDefined();

    const version = registerResponse?.version!;

    const updateCompatibilityResponse: Compatibility = await schemaRegistryClient.updateCompatibility(testSubject, Compatibility.BACKWARD_TRANSITIVE);
    expect(updateCompatibilityResponse).toEqual(Compatibility.BACKWARD_TRANSITIVE);

    const getCompatibilityResponse: Compatibility = await schemaRegistryClient.getCompatibility(testSubject);
    expect(getCompatibilityResponse).toEqual(Compatibility.BACKWARD_TRANSITIVE);

    const testSubjectCompatibilityResponse: boolean = await schemaRegistryClient.testSubjectCompatibility(testSubject, backwardCompatibleSchemaInfo);
    expect(testSubjectCompatibilityResponse).toEqual(true);

    const testCompatibilityResponse: boolean = await schemaRegistryClient.testCompatibility(testSubject, version, backwardCompatibleSchemaInfo);
    expect(testCompatibilityResponse).toEqual(true);
  });

  it('Should update and get default compatibility', async () => {
    const updateDefaultCompatibilityResponse: Compatibility = await schemaRegistryClient.updateDefaultCompatibility(Compatibility.FULL);
    expect(updateDefaultCompatibilityResponse).toEqual(Compatibility.FULL);

    const getDefaultCompatibilityResponse: Compatibility = await schemaRegistryClient.getDefaultCompatibility();
    expect(getDefaultCompatibilityResponse).toEqual(Compatibility.FULL);
  });

  it('Should update and get subject Config', async () => {
    const subjectConfigRequest: ServerConfig = {
      compatibility: Compatibility.FULL,
      normalize: true
    };

    const subjectConfigResponse: ServerConfig = {
      compatibilityLevel: Compatibility.FULL,
      normalize: true
    };

    const registerResponse:  SchemaMetadata = await schemaRegistryClient.registerFullResponse(testServerConfigSubject, schemaInfo);
    expect(registerResponse).toBeDefined();

    const updateConfigResponse: ServerConfig = await schemaRegistryClient.updateConfig(testServerConfigSubject, subjectConfigRequest);
    expect(updateConfigResponse).toBeDefined();

    const getConfigResponse: ServerConfig = await schemaRegistryClient.getConfig(testServerConfigSubject);
    expect(getConfigResponse).toEqual(subjectConfigResponse);
  });

  it('Should get and set default Config', async () => {
    const serverConfigRequest: ServerConfig = {
      compatibility: Compatibility.FULL,
      normalize: false
    };

    const serverConfigResponse: ServerConfig = {
      compatibilityLevel: Compatibility.FULL,
      normalize: false
    };

    const updateDefaultConfigResponse: ServerConfig = await schemaRegistryClient.updateDefaultConfig(serverConfigRequest);
    expect(updateDefaultConfigResponse).toBeDefined();

    const getDefaultConfigResponse: ServerConfig = await schemaRegistryClient.getDefaultConfig();
    expect(getDefaultConfigResponse).toEqual(serverConfigResponse);
  });

});
