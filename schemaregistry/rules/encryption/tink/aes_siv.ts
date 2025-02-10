/**
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import {Aead} from './aead';

// @ts-expect-error miscreant does not have types
import {SIV, SoftCryptoProvider} from "@hackbg/miscreant-esm";

/**
 * Implementation of AES-SIV.
 *
 */
export class AesSiv extends Aead {
  constructor(private readonly key: Uint8Array) {
    super();
  }

  /**
   */
  async encrypt(plaintext: Uint8Array, associatedData?: Uint8Array):
      Promise<Uint8Array> {
    let key = await SIV.importKey(this.key, "AES-CMAC-SIV", new SoftCryptoProvider());
    return key.seal(plaintext, associatedData != null ? [associatedData] : []);
  }

  /**
   */
  async decrypt(ciphertext: Uint8Array, associatedData?: Uint8Array):
      Promise<Uint8Array> {
    let key = await SIV.importKey(this.key, "AES-CMAC-SIV", new SoftCryptoProvider());
    return key.open(ciphertext, associatedData != null? [associatedData] : []);
  }
}

export async function fromRawKey(key: Uint8Array): Promise<Aead> {
  return new AesSiv(key);
}
