import { MOCK_TS } from "../../dekregistry/constants";
import { Kek, Dek } from "../../dekregistry/dekregistry-client";

const TEST_KEK_NAME: string = 'test-kek-name';
const TEST_KEK_NAME_2: string = 'test-kek-name2';
const TEST_KMS_TYPE: string = 'test-kms-type';
const TEST_KMS_KEY_ID: string = 'test-kms-key-id';
const TEST_KMS_PROPS = { testKey: 'testValue' };
const TEST_DOC: string = 'test-doc';

const TEST_SUBJECT: string = 'test-subject';
const TEST_ALGORITHM: string = 'test-algorithm';
const TEST_ENCRYPTED_KEY_MATERIAL: string = 'test-encrypted-key-material';
const TEST_VERSION: number = 1;

const TEST_KEK: Kek = {
  name: TEST_KEK_NAME,
  kmsType: TEST_KMS_TYPE,
  kmsKeyId: TEST_KMS_KEY_ID,
  kmsProps: TEST_KMS_PROPS,
  doc: TEST_DOC,
  shared: true
};

const TEST_KEK_2: Kek = {
  name: TEST_KEK_NAME_2,
  kmsType: TEST_KMS_TYPE,
  kmsKeyId: TEST_KMS_KEY_ID,
  kmsProps: TEST_KMS_PROPS,
  doc: TEST_DOC,
  shared: true
};

const TEST_DEK: Dek = {
  kekName: TEST_KEK_NAME,
  subject: TEST_SUBJECT,
  algorithm: TEST_ALGORITHM,
  encryptedKeyMaterial: TEST_ENCRYPTED_KEY_MATERIAL,
  version: TEST_VERSION,
  ts: MOCK_TS
};

const TEST_DEK_V2: Dek = {
  kekName: TEST_KEK_NAME,
  subject: TEST_SUBJECT,
  algorithm: TEST_ALGORITHM,
  encryptedKeyMaterial: TEST_ENCRYPTED_KEY_MATERIAL,
  version: 2,
  ts: MOCK_TS
};

const TEST_DEK_2: Dek = {
  kekName: TEST_KEK_NAME_2,
  subject: TEST_SUBJECT,
  algorithm: TEST_ALGORITHM,
  encryptedKeyMaterial: TEST_ENCRYPTED_KEY_MATERIAL,
  version: TEST_VERSION,
  ts: MOCK_TS
};

const TEST_DEK_LATEST: Dek = {
  kekName: TEST_KEK_NAME,
  subject: TEST_SUBJECT,
  algorithm: TEST_ALGORITHM,
  encryptedKeyMaterial: TEST_ENCRYPTED_KEY_MATERIAL,
  version: -1,
  ts: MOCK_TS
};

export {
  TEST_KEK_NAME, TEST_KEK_NAME_2, TEST_KMS_TYPE, TEST_KMS_KEY_ID, TEST_KMS_PROPS, TEST_DOC,
  TEST_SUBJECT, TEST_ALGORITHM, TEST_ENCRYPTED_KEY_MATERIAL, TEST_VERSION,
  TEST_KEK, TEST_KEK_2, TEST_DEK, TEST_DEK_V2, TEST_DEK_2, TEST_DEK_LATEST
};