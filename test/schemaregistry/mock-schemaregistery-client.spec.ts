import { MockClient } from '../../schemaregistry/mock-schemaregistry-client';
import { Compatibility, Metadata, SchemaInfo, SchemaMetadata } from '../../schemaregistry/schemaregistry-client';
import { RestService } from '../../schemaregistry/rest-service';
import { beforeEach, afterEach, describe, expect, it, jest } from '@jest/globals';

const schemaString: string = JSON.stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' },
  ],
});

const schemaString2: string = JSON.stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' },
    { name: 'email', type: 'string' },
  ],
});

const metadata: Metadata = {
  properties: {
    owner: 'Alice Bob',
    email: 'alice@bob.com',
  }
};

const metadata2: Metadata = {
  properties: {
    owner: 'Alice Bob2',
    email: 'alice@bob2.com'
  }
};

const metadataKeyValue: { [key: string]: string } = {
  owner: 'Alice Bob',
  email: 'alice@bob.com'
};

const metadataKeyValue2: { [key: string]: string } = {
  owner: 'Alice Bob2',
  email: 'alice@bob2.com'
};

const schemaInfo: SchemaInfo = {
  schema: schemaString,
  metadata: metadata
};

const schemaInfo2: SchemaInfo = {
  schema: schemaString2,
  metadata: metadata2
};

const testSubject = 'test-subject';
const testSubject2 = 'test-subject2';


describe('MockClient-tests', () => {
  let mockClient: MockClient;
  let restService: RestService;

  beforeEach(() => {
    mockClient = new MockClient();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return schemaId when calling register', async () => {
    const response: number = await mockClient.register(testSubject, schemaInfo);
    expect(response).toBe(1);
  });

  it('Should return SchemaMetadata when calling registerFullResponse', async () => {
    const response: SchemaMetadata = await mockClient.registerFullResponse(testSubject, schemaInfo);
    expect(response.id).toBe(1);
  });

  it('Should return SchemaInfo when getting with subject and Id', async () => {
    await mockClient.register(testSubject, schemaInfo);
    const response: SchemaInfo = await mockClient.getBySubjectAndId(testSubject, 1);
    expect(response.schema).toBe(schemaString);
  });

  it('Should throw error when getBySubjectAndId is called with non-existing schemaId', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await expect(mockClient.getBySubjectAndId(testSubject, 2)).rejects.toThrowError();
  });

  it('Should return schemaId when calling getId', async () => {
    await mockClient.register(testSubject, schemaInfo);
    const response: number = await mockClient.getId(testSubject, schemaInfo);
    expect(response).toBe(1);
  });

  it('Should throw error when getId is called with non-existing schema', async () => {
    await expect(mockClient.getId(testSubject, schemaInfo)).rejects.toThrowError();
  });

  it('Should return latest schema metadata when calling getLatestSchemaMetadata', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject, schemaInfo2);
    const response: SchemaMetadata = await mockClient.getLatestSchemaMetadata(testSubject);
    expect(response.id).toBe(2);
    expect(response.schema).toBe(schemaString2);
  });

  it('Should return latest Schema with metadata when calling getLatestWithMetadata', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject, schemaInfo2);
    const response = await mockClient.getLatestWithMetadata(testSubject, metadataKeyValue);
    expect(response.schema).toBe(schemaString);
    expect(response.version).toBe(1);
    const response2 = await mockClient.getLatestWithMetadata(testSubject, metadataKeyValue2);
    expect(response2.schema).toBe(schemaString2);
    expect(response2.version).toBe(2);
  });

  it('Should return specific schemaMetadata version when calling getSchemaMetadata', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject, schemaInfo2);
    const response: SchemaMetadata = await mockClient.getSchemaMetadata(testSubject, 1);
    expect(response.id).toBe(1);
    expect(response.schema).toBe(schemaString);
    const response2: SchemaMetadata = await mockClient.getSchemaMetadata(testSubject, 2);
    expect(response2.id).toBe(2);
    expect(response2.schema).toBe(schemaString2);
  });

  it('Should return the correct version when calling getVersion', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject, schemaInfo2);
    const response: number = await mockClient.getVersion(testSubject, schemaInfo2);
    expect(response).toBe(2);
  });

  it('Should throw error when getVersion is called with non-existing schema', async () => {
    await expect(mockClient.getVersion(testSubject, schemaInfo)).rejects.toThrowError();
  });

  it('Should return all versions when calling getAllVersions', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject, schemaInfo2);
    const response: number[] = await mockClient.getAllVersions(testSubject);
    expect(response).toEqual([1, 2]);
  });

  it('Should update compatibility when calling updateCompatibility', async () => {
    const response: Compatibility = await mockClient.updateCompatibility(testSubject, Compatibility.BackwardTransitive);
    expect(response).toBe(Compatibility.BackwardTransitive);
  });

  it('Should return compatibility when calling getCompatibility', async () => {
    await mockClient.updateCompatibility(testSubject, Compatibility.BackwardTransitive);
    const response: Compatibility = await mockClient.getCompatibility(testSubject);
    expect(response).toBe(Compatibility.BackwardTransitive);
  });

  it('Should throw error when getCompatibility is called with non-existing subject', async () => {
    await expect(mockClient.getCompatibility(testSubject)).rejects.toThrowError();
  });

  it('Should update default compatibility when calling updateDefaultCompatibility', async () => {
    const response: Compatibility = await mockClient.updateDefaultCompatibility(Compatibility.BackwardTransitive);
    expect(response).toBe(Compatibility.BackwardTransitive);
  });

  it('Should return default compatibility when calling getDefaultCompatibility', async () => {
    await mockClient.updateDefaultCompatibility(Compatibility.BackwardTransitive);
    const response: Compatibility = await mockClient.getDefaultCompatibility();
    expect(response).toBe(Compatibility.BackwardTransitive);
  });

  it('Should throw error when getDefaultCompatibility is called with non-existing default compatibility', async () => {
    await expect(mockClient.getDefaultCompatibility()).rejects.toThrowError();
  });

  it('Should get all subjects when calling getAllSubjects', async () => {
    expect(await mockClient.getAllSubjects()).toEqual([]);

    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject2, schemaInfo);
    const response: string[] = await mockClient.getAllSubjects();
    expect(response).toEqual([testSubject, testSubject2]);
  });

  it('Should soft delete subject when calling deleteSubject', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.deleteSubject(testSubject);
    await expect(mockClient.getId(testSubject, schemaInfo)).rejects.toThrowError();
    await expect(mockClient.getVersion(testSubject, schemaInfo)).rejects.toThrowError();
    const response: SchemaInfo = await mockClient.getBySubjectAndId(testSubject, 1);
    await expect(response.schema).toBe(schemaString);
  });

  it('Should permanent delete subject when calling deleteSubject with permanent flag', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.deleteSubject(testSubject, true);
    await expect(mockClient.getId(testSubject, schemaInfo)).rejects.toThrowError();
    await expect(mockClient.getVersion(testSubject, schemaInfo)).rejects.toThrowError();
    await expect(mockClient.getBySubjectAndId(testSubject, 1)).rejects.toThrowError();
  });

  it('Should soft delete subject version when calling deleteSubjectVersion', async () => {
    await mockClient.register(testSubject, schemaInfo);
    await mockClient.register(testSubject, schemaInfo2);
    await mockClient.deleteSubjectVersion(testSubject, 1);
    await expect(mockClient.getId(testSubject, schemaInfo)).rejects.toThrowError();
    await expect(mockClient.getVersion(testSubject, schemaInfo)).rejects.toThrowError();
    const response: SchemaInfo = await mockClient.getBySubjectAndId(testSubject, 1);
    await expect(response.schema).toBe(schemaString);
    const response2: SchemaInfo = await mockClient.getBySubjectAndId(testSubject, 2);
    await expect(response2.schema).toBe(schemaString2);
  });
});