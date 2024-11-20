/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *               2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var util = module.exports = {};

util.shallowCopy = function (obj) {

  if (!util.isObject(obj)) { return obj; }

  var copy = {};

  for (var k in obj) {
    if (Object.hasOwn(obj, k)) {
      copy[k] = obj[k];
    }
  }

  return copy;
};

util.isObject = function (obj) {
  return obj && typeof obj === 'object';
};

// Convert Map or object to a list of [key, value, key, value...].
util.dictToStringList = function (mapOrObject) {
  let list = null;
  if (mapOrObject && (mapOrObject instanceof Map)) {
    list =
      Array
        .from(mapOrObject).reduce((acc, [key, value]) => {
          acc.push(key, value);
          return acc;
        }, [])
        .map(v => String(v));
  } else if (util.isObject(mapOrObject)) {
    list =
      Object
        .entries(mapOrObject).reduce((acc, [key, value]) => {
          acc.push(key, value);
          return acc;
        }, [])
        .map(v => String(v));
  }
  return list;
};

util.bindingVersion = '0.5.1';
