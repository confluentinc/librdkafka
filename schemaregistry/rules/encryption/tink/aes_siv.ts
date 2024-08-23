/**
 * @license
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import {Aead} from './aead';

// @ts-expect-error miscreant does not have types
import {AEAD} from "@hackbg/miscreant-esm";

/**
 * Implementation of AES-SIV.
 *
 * @final
 */
export class AesSiv extends Aead {
  constructor(private readonly key: Uint8Array) {
    super();
  }

  /**
   */
  async encrypt(plaintext: Uint8Array, associatedData?: Uint8Array):
      Promise<Uint8Array> {
    let key = await AEAD.importKey(this.key, "AES-CMAC-SIV");
    return key.seal(plaintext, null, associatedData);
  }

  /**
   */
  async decrypt(ciphertext: Uint8Array, associatedData?: Uint8Array):
      Promise<Uint8Array> {
    let key = await AEAD.importKey(this.key, "AES-CMAC-SIV");
    return key.open(ciphertext, null, associatedData);
  }
}

export async function fromRawKey(key: Uint8Array): Promise<Aead> {
  return new AesSiv(key);
}
