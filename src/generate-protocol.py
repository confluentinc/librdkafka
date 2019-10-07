#!/usr/bin/env python
#
# Parse the Apache Kafka protocol specification files and generate a
# protocol request and field tree in C.
#
# Usage:
#  $ src/generate-protocol.py ~/src/kafka/clients/src/main/resources/common/message/*.json
#

import sys
import os
import json
import re


def ind(steps):
    """ Indent """
    return ' ' * (steps * 8)


class Field(object):
    VER_MAX = 1000

    c_bool = {True: 'rd_true', False: 'rd_false'}

    c_ftypes = { None: 'RD_KAFKAP_FIELD_TYPE_ROOT',
                 'string': 'RD_KAFKAP_FIELD_TYPE_STR',
                 'int8': 'RD_KAFKAP_FIELD_TYPE_INT8',
                 'int16': 'RD_KAFKAP_FIELD_TYPE_INT16',
                 'int32': 'RD_KAFKAP_FIELD_TYPE_INT32',
                 'int64': 'RD_KAFKAP_FIELD_TYPE_INT64',
                 'bool': 'RD_KAFKAP_FIELD_TYPE_BOOL',
                 'bytes': 'RD_KAFKAP_FIELD_TYPE_BYTES'}



    def __init__(self, parname, name, ftype, versions):
        self.name = name
        if name != '':
            self.varname = '{}_{}'.format(parname, name)
        else:
            # root node does not have a name
            self.varname = '{}{}'.format(parname, name)
        self.ftype = ftype
        self.fields = []  # subfields
        self.array = False
        self.nullable = False
        self.versions = versions

    @staticmethod
    def parse_versions(sversions):
        """ Parses a version range string into a min,max tuple.
            Supported formats:
                ''    -> None
                '2'   -> (2,2)
                '0+'  -> (0,VER_MAX)
                '2-4' -> (2,3,4)
        """

        if sversions is None or len(sversions) == 0:
            return None

        m = re.match(r'^(\d+)\+$', sversions)
        if m:
            return (int(m.group(1)), Field.VER_MAX)

        m = re.match(r'^(\d+)-(\d+)$', sversions)
        if m:
            return (int(m.group(1)), int(m.group(2)))

        m = re.match(r'^(\d+)$', sversions)
        if m:
            return (int(m.group(1)), int(m.group(1)))

        raise Exception("Unable to parse versions {}".format(sversions))

    @staticmethod
    def parse_type(ftype):
        if ftype.startswith('[]'):
            return (ftype[2:], True)
        else:
            return (ftype, False)

    @staticmethod
    def parse_spec(parname, jspec, version):
        versions = Field.parse_versions(jspec.get('versions'))

        if version < versions[0] or version > versions[1]:
            # Field not valid for this version
            return None

        name = jspec.get('name')
        ftype, array = Field.parse_type(jspec.get('type'))

        field = Field(parname, name, ftype, versions)
        field.array = array
        nullableVersions = Field.parse_versions(jspec.get('nullableVersions'))

        field.nullable = nullableVersions is not None and version >= nullableVersions[0] and version <= nullableVersions[1]

        # Parse subfields, if any.
        for subfieldspec in jspec.get('fields', []):
            subfield = field.parse_spec(field.varname, subfieldspec, version)
            if subfield is not None:
                field.fields.append(subfield)

        return field

    def write_c_fwd_decl(self, fp):
        fp.write("extern const rd_kafkap_field_t %s;\n" % self.varname)
        for subfield in self.fields:
            subfield.write_c_fwd_decl(fp)

    def write_c_def(self, fp):
        """ Write definition of field """
        indent = 1

        if self.array:
            cftype = 'RD_KAFKAP_FIELD_TYPE_ARRAY /* []{} */'.format(self.ftype)
        else:
            cftype = self.c_ftypes[self.ftype]


        if True or self.ftype is not None:
            fp.write("const rd_kafkap_field_t %s = {\n" % self.varname)
            fp.write(ind(indent) + ".name = \"%s\",\n" % (self.name))
            fp.write(ind(indent) + ".ftype = %s,\n" % (cftype))
            fp.write(ind(indent) + ".nullable = %s,\n" % (self.c_bool[self.nullable]))

            if len(self.fields) > 0:
                fp.write(ind(indent) + ".field_cnt = %d,\n" % len(self.fields))
                fp.write(ind(indent) + ".fields = {\n")

                for subfield in self.fields:
                    fp.write(ind(indent+1) + "&%s,\n" % subfield.varname)

                fp.write(ind(indent) + "},\n")

        if True or self.ftype is not None:
            fp.write("};\n\n")

        # Now let the subfields to the same thing
        for subfield in self.fields:
            subfield.write_c_def(fp)


    def write_c(self, fp, indent):
        if len(self.fields) == 0:
            return

        fp.write(ind(indent+1) + "{\n")
        fp.write(ind(indent+2) + ".field_cnt = %d,\n" % len(self.fields))
        fp.write(ind(indent+2) + ".fields = {\n")

        for subfield in self.fields:
            fp.write(ind(indent+3) + "&%s,\n" % subfield.varname)

        fp.write(ind(indent+2) + "},\n")
        fp.write(ind(indent+1) + "},\n")


class Request(object):
    def __init__(self, apiKey, name, rtype, path=None):
        self.name = name.split('Request')[0]
        self.apiKeyName = "RD_KAFKAP_{}".format(self.name)
        self.apiKey = apiKey
        self.path = path
        self.req_versions = []
        self.resp_versions = []

    @staticmethod
    def parse_spec(reqs, path, jspec):
        name = jspec.get('name')
        m = re.match(r'^(\w+)(Request|Response)$', name)
        if not m:
            raise Exception("Unable to deduce Request or Response type from name {} in {}".format(name, os.path.basename(path)))

        name = m.group(1)
        rtype = m.group(2)
        is_response = rtype == 'Response'

        apiKey = int(jspec.get('apiKey'))

        req = reqs.get(apiKey, None)
        if req is None:
            req = Request(int(jspec.get('apiKey')), jspec.get('name'),
                      rtype, path)

            reqs[req.apiKey] = req
            req.validVersions = Field.parse_versions(jspec.get('validVersions'))

            req.req_versions = dict()
            req.resp_versions = dict()

        for version in range(req.validVersions[0], req.validVersions[1]+1):
            root = Field("rd_kafkap_field_{}{}V{}".format(name, rtype, version), "", None, (version, version))
            for jfield in jspec.get('fields'):
                field = Field.parse_spec(root.varname, jfield, version)
                if field is not None:
                    root.fields.append(field)

            if is_response:
                req.resp_versions[version] = root
            else:
                req.req_versions[version] = root

        return req


    def write_c_fwd_decl(self, fp):
        """ Write forward declarations for all request fields """
        for version in sorted(self.req_versions):
            root = self.req_versions[version]
            root.write_c_fwd_decl(fp)
        for version in sorted(self.resp_versions):
            root = self.resp_versions[version]
            root.write_c_fwd_decl(fp)

    def write_c_def(self, fp):
        """ Write field definitions """
        for version in sorted(self.req_versions):
            root = self.req_versions[version]
            root.write_c_def(fp)

        for version in sorted(self.resp_versions):
            root = self.resp_versions[version]
            root.write_c_def(fp)

    def write_c(self, fp, indent=1):
        assert self.req_versions.keys() == self.resp_versions.keys(), \
            "req versions: {}, resp versions: {}".format(
                self.req_versions, self.resp_versions)

        fp.write(ind(indent) + "/* {} */\n".format(
            self.name, os.path.basename(self.path)))
        fp.write(ind(indent) + "{\n")
        fp.write(ind(indent+1) + ".apiKey = %s,\n" % (self.apiKeyName))
        fp.write(ind(indent+1) + ".name   = \"%s\",\n" % (self.name))
        fp.write(ind(indent+1) + ".minver = %d,\n" % self.validVersions[0])
        fp.write(ind(indent+1) + ".maxver = %d,\n" % self.validVersions[1])
        fp.write(ind(indent+1) + ".req_versions = {\n")
        for version in sorted(self.req_versions):
            root = self.req_versions[version]
            fp.write(ind(indent+2) + "&{},\n".format(root.varname))
        fp.write(ind(indent+1) + "},\n")
        fp.write(ind(indent+1) + ".resp_versions = {\n")
        for version in sorted(self.resp_versions):
            root = self.req_versions[version]
            fp.write(ind(indent+2) + "&{},\n".format(root.varname))
        fp.write(ind(indent+1) + "},\n")
        fp.write(ind(indent) + "},\n")


def write_license(fp):
    fp.write("""/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @warning THIS FILE WAS AUTOMATICALLY GENERATED BY generate-protocol.py
 *          DO NOT EDIT!
 */

""")

def write_h(fp, reqs):
    version_max = max([x.validVersions[1] for x in reqs])

    write_license(fp)

    fp.write("""
#ifndef _RDKAFKA_PROTO_REQUESTS_H_
#define _RDKAFKA_PROTO_REQUESTS_H_

/**
 * @brief Apache Kafka protocol requests
 */
typedef enum {
#define RD_KAFKAP_None -1
""")
    for req in reqs:
        fp.write(ind(1) + "{} = {},\n".format(req.apiKeyName, req.apiKey))

    fp.write("#define RD_KAFKAP__CNT (%s+1)\n" % reqs[-1].apiKeyName)
    fp.write("""
} rd_kafkap_request_type_t;


/**
 * @brief Protocol field types
 */
typedef enum {
        RD_KAFKAP_FIELD_TYPE_INVALID,
        RD_KAFKAP_FIELD_TYPE_ROOT,
        RD_KAFKAP_FIELD_TYPE_INT8,
        RD_KAFKAP_FIELD_TYPE_INT16,
        RD_KAFKAP_FIELD_TYPE_INT32,
        RD_KAFKAP_FIELD_TYPE_INT64,
        RD_KAFKAP_FIELD_TYPE_BOOL,
        RD_KAFKAP_FIELD_TYPE_STR,
        RD_KAFKAP_FIELD_TYPE_BYTES,
        RD_KAFKAP_FIELD_TYPE_ARRAY,

        RD_KAFKAP_FIELD_TYPE__CNT
} rd_kafkap_field_type_t;

/**
 * @returns the human readable name for a field type
 */
static RD_UNUSED const char *
rd_kafkap_field_type2str (rd_kafkap_field_type_t ftype) {
        static const char *names[] = {
                "invalid",
                "root",
                "int8",
                "int16",
                "int32",
                "int64",
                "bool",
                "string",
                "bytes",
                "array"
        };

        rd_assert(ftype >= 0 && ftype < RD_KAFKAP_FIELD_TYPE__CNT);
        return names[ftype];
}


/**
 * @brief Protoco field
 */
typedef struct rd_kafkap_field_s {
        const char               *name;
        rd_kafkap_field_type_t    ftype;
        rd_bool_t                 nullable;
        size_t                    field_cnt;
        const struct rd_kafkap_field_s *fields[];   /**< Sub-fields */
} rd_kafkap_field_t;

/**
 * @brief Maximum ApiVersion in any request
 */
""")
    fp.write("#define RD_KAFKAP__VERSION_MAX %d\n" % version_max)

    fp.write("""

/**
 * @struct An Apache Kafka protocol request or response definition
 */
typedef struct rd_kafkap_request_s {
        int16_t     apiKey;
        const char *name;
        int16_t     minver;
        int16_t     maxver;
        const rd_kafkap_field_t *req_versions[RD_KAFKAP__VERSION_MAX+1]; /**< Request versions */
        const rd_kafkap_field_t *resp_versions[RD_KAFKAP__VERSION_MAX+1]; /**< Response versions */
} rd_kafkap_request_t;

/**< The protocol definition tree */
extern const rd_kafkap_request_t rd_kafkap_requests[RD_KAFKAP__CNT];

#endif /* _RDKAFKA_PROTO_REQUESTS_H_ */
""")


def write_c(fp, reqs):
    write_license(fp)

    fp.write("""
/**
 * @brief Forward declarations
 */
""")
    for req in reqs:
        req.write_c_fwd_decl(fp)

    fp.write("""
/**
 * @brief Field definitions
 */
""")

    for req in reqs:
        req.write_c_def(fp)


    fp.write("""
/**
 * @name Protocol request/response definitions
 *
 */
const rd_kafkap_request_t rd_kafkap_requests[RD_KAFKAP__CNT] = {
""")

    for req in reqs:
        req.write_c(fp, 1)

    fp.write("};\n")


if __name__ == '__main__':

    reqs = {}

    for path in sys.argv[1:]:
        file = os.path.basename(path)

        m = re.match(r'^(\w+)(Request|Response|Header).json$', file)
        if not m:
            raise Exception("{} does not seem to be a protocol specification file".
                            format(path))

        name = m.group(1)
        rtype = m.group(2)

        if rtype == 'Header':
            continue

        with open(path, 'r') as fp:
            blob = ''
            for line in fp:
                if re.match(r'^\s*//', line):
                    continue

                blob += line

        jspec = json.loads(blob)

        Request.parse_spec(reqs, path, jspec)


    # Sort by apiKey
    def cmp_apiKey(a, b):
        return cmp(a.apiKey, b.apiKey)

    sorted_reqs = sorted(reqs.values(), cmp=cmp_apiKey)

    output_prefix = "src/rdkafka_proto_requests"

    with open("{}.h".format(output_prefix), "w") as fp:
        write_h(fp, sorted_reqs)

    with open("{}.c".format(output_prefix), "w") as fp:
        write_c(fp, sorted_reqs)
