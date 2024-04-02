/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var util = module.exports = {};

util.shallowCopy = function (obj) {

  if (!util.isObject(obj)) { return obj; }

  var copy = {};

  for (var k in obj) {
    if (obj.hasOwnProperty(k)) {
      copy[k] = obj[k];
    }
  }

  return copy;
};

util.isObject = function (obj) {
  return obj && typeof obj === 'object';
};

util.bindingVersion = 'v0.1.11-devel';
