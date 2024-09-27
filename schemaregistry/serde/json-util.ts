/*
 * Copyright (c) 2023 Menglin "Mark" Xu <mark@remarkablemark.org>
 *           (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
import validator from 'validator';
import { deepStrictEqual } from 'assert';

/**
 * Generate JSON schema from value.
 *
 * @param value - Value.
 * @returns JSON schema.
 */
export function generateSchema(value: any): any {
  switch (true) {
    case value === undefined:
    case typeof value === 'undefined':
    case typeof value === 'function':
    case typeof value === 'symbol':
    case value instanceof Date:
      throw new TypeError(`Invalid JSON value: ${String(value)}`);

    /**
     * @see https://json-schema.org/understanding-json-schema/reference/null.html
     */
    case value === null:
      return { type: 'null' };

    /**
     * @see https://json-schema.org/understanding-json-schema/reference/numeric.html
     */
    case typeof value === 'number':
      return { type: Number.isInteger(value) ? 'integer' : 'number' };

    /**
     * @see https://json-schema.org/understanding-json-schema/reference/boolean.html
     */
    case typeof value === 'boolean':
      return { type: 'boolean' };

    /**
     * @see https://json-schema.org/understanding-json-schema/reference/string.html
     */
    case typeof value === 'string':
      if (validator.isISO8601(value)) {
        return {
          type: 'string',
          format: value.includes('T') ? 'date-time' : 'date',
        };
      }

      if (validator.isTime(value.split('+')[0], { mode: 'withSeconds' })) {
        return { type: 'string', format: 'time' };
      }

      if (validator.isEmail(value)) {
        return { type: 'string', format: 'email' };
      }

      return { type: 'string' };

    /**
     * @see https://json-schema.org/understanding-json-schema/reference/array.html
     */
    case Array.isArray(value):
      if (value.length === 1) {
        return { type: 'array', items: generateSchema(value[0]) };
      }

      if (value.length > 1) {
        const items = value.map(generateSchema);
        if (deepEqual(...items)) {
          return { type: 'array', items: items[0] };
        }
      }

      return { type: 'array' };

    /**
     * @see https://json-schema.org/understanding-json-schema/reference/object.html
     */
    case value instanceof Object:
      if (!Object.keys(value).length) {
        return { type: 'object' };
      }

      return {
        type: 'object',
        properties: Object.entries(value).reduce(
          (accumulator, [key, value]) => {
            accumulator[key] = generateSchema(value);
            return accumulator;
          },
          {} as Record<string, any>,
        ),
      };

    /* istanbul ignore next */
    default:
      throw new TypeError(`Invalid JSON value: ${value}`);
  }
}

/**
 * Tests for deep equality between the `actual` and `expected` parameters.
 */
export function deepEqual(...args: unknown[]): boolean {
  try {
    for (let index = 0, count = args.length; index < count; index++) {
      if (index + 1 === count) {
        continue;
      }
      deepStrictEqual(args[index], args[index + 1]);
    }
    return true;
  } catch (error) {
    return false;
  }
}
