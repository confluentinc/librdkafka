import { Dek, Kek } from "../../dekregistry/dekregistry-client";
import { MockDekRegistryClient } from "../../dekregistry/mock-dekregistry-client";
import { beforeEach, afterEach, describe, expect, it, jest } from '@jest/globals';
import { TEST_KEK, TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, 
  TEST_KMS_PROPS, TEST_DOC, TEST_DEK, TEST_DEK_V2, TEST_DEK_LATEST, TEST_ALGORITHM, 
  TEST_ENCRYPTED_KEY_MATERIAL, TEST_SUBJECT, TEST_VERSION } from "./test-constants";

describe('MockClient-tests', () => {
    let mockClient: MockDekRegistryClient;
  
    beforeEach(() => {
      mockClient = new MockDekRegistryClient();
    });
  
    afterEach(() => {
      jest.clearAllMocks();
    });
  
    it('Should return kek when registering Kek', async () => {
      const registerKekResponse: Kek = await mockClient.registerKek(
        TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);

      expect(registerKekResponse).toEqual(TEST_KEK);
    });

    it('Should return kek when getting Kek', async () => {
      await mockClient.registerKek(TEST_KEK_NAME, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC, true);
      const getKekResponse: Kek = await mockClient.getKek(TEST_KEK_NAME);

      expect(getKekResponse).toEqual(TEST_KEK);
    });

    it('Should return dek when registering Dek', async () => {
      const registerDekResponse: Dek = await mockClient.registerDek(
        TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);

      expect(registerDekResponse).toEqual(TEST_DEK);
    });

    it('Should return dek when getting Dek', async () => {
      await mockClient.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
      const getDekResponse: Dek = await mockClient.getDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_VERSION);

      expect(getDekResponse).toEqual(TEST_DEK);
    });

    it('Should return latest dek when getting Dek with version -1', async () => {
      await mockClient.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, 2);
      await mockClient.registerDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION);
      const getDekResponse: Dek = await mockClient.getDek(TEST_KEK_NAME, TEST_SUBJECT, TEST_ALGORITHM, -1);

      expect(getDekResponse).toEqual(TEST_DEK_V2);
    });
  });