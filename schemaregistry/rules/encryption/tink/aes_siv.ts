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
  constructor(private readonly key: Uint8Array<ArrayBuffer>) {
    super();
  }

  /**
   */
  async encrypt(plaintext: Uint8Array<ArrayBuffer>, associatedData?: Uint8Array<ArrayBuffer>):
      Promise<Uint8Array<ArrayBuffer>> {
    let key = await SIV.importKey(this.key, "AES-CMAC-SIV", new SoftCryptoProvider());
    return key.seal(plaintext, associatedData != null ? [associatedData] : []);
  }

  /**
   */
  async decrypt(ciphertext: Uint8Array<ArrayBuffer>, associatedData?: Uint8Array<ArrayBuffer>):
      Promise<Uint8Array<ArrayBuffer>> {
    let key = await SIV.importKey(this.key, "AES-CMAC-SIV", new SoftCryptoProvider());
    return key.open(ciphertext, associatedData != null? [associatedData] : []);
  }
}

export async function fromRawKey(key: Uint8Array<ArrayBuffer>): Promise<Aead> {
  return new AesSiv(key);
}
