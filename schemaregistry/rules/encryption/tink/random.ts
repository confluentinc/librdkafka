/**
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */


/**
 * Several simple wrappers of crypto.getRandomValues.
 */
import {InvalidArgumentsException} from './exception/invalid_arguments_exception';
import * as crypto from 'crypto';

/**
 * Randomly generates `n` bytes.
 *
 * @param n - number of bytes to generate
 * @returns the random bytes
 */
export function randBytes(n: number): Uint8Array<ArrayBuffer> {
  if (!Number.isInteger(n) || n < 0) {
    throw new InvalidArgumentsException('n must be a nonnegative integer');
  }
  const result = new Uint8Array(n);
  crypto.getRandomValues(result);
  return result;
}
