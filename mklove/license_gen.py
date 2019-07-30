#!/usr/bin/env python
#
# Copyright (c) 2019, Magnus Edenhill
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
import sys
import re
from hashlib import sha1


licenses = dict()  # keyed by sha1:d normalized license, value is License


class License(object):
    def __init__(self, score, normalized, full):
        self.score = score
        self.normalized = normalized
        self.full = full
        self.files = []
        self.sha = sha1(normalized).hexdigest()

    def add_file(self, path):
        self.files.append(path)

    @staticmethod
    def cmp_score(a, b):
        return a.score - b.score

    @staticmethod
    def calc_score(normalized):
        trigger_words = ['redistribution', 'copyright', 'this software']
        return sum([1 for x in trigger_words if normalized.find(x) != -1])


def normalize_comment(comment):
    s = comment
    s = re.sub(r'^/\*{1,}', '', s)
    s = re.sub(r'\*{1,}/$', '', s)
    s = re.sub(r'^\s+', '', s, flags=re.MULTILINE)
    s = re.sub(r'\s+$', '', s, flags=re.MULTILINE)
    s = re.sub(r'^\s*\*', '', s, flags=re.MULTILINE)
    readable = s
    s = re.sub(r'\n{2,}', '\n', s)
    s = re.sub(r'[ \t]{2,}', ' ', s)
    s = re.sub(r'copyright (\(c\) )?\d+(-\d+)?.*', 'copyright', s, flags=re.IGNORECASE)
    return (readable, s.lower().strip())


def get_license(path, max_comments=5):
    with open(path, 'r') as f:
        data = f.read()

    matched = []
    cnt = 0

    for m in re.finditer(r'\/\*(\*(?!\/)|[^*])*\*\/', data, re.MULTILINE):
        readable, can = normalize_comment(m.group(0))
        score = License.calc_score(can)
        if score < 1:
            continue

        license = License(score, can, readable)
        if license is None:
            continue

        matched.append(license)

        cnt += 1
        if cnt >= max_comments:
            break

    assert len(matched) > 0, "No eligible licenses matched in {}".format(path)

    best = sorted(matched, License.cmp_score)[0]

    license = licenses.get(best.sha, None)
    if license is None:
        license = License(best.score, best.normalized, best.full)
        licenses[best.sha] = license

    license.add_file(path)

    return license


if __name__ == '__main__':
    files = sys.argv[1:]
    for path in files:
        get_license(path)

    print("LICENSE SUMMARY")
    print("Summary of {} license(s) in {} source files".format(
        len(licenses), len(files)))
    print("\n")

    for license in sorted(licenses.itervalues()):
        print("=" * 79)
        print(license.full)
        print("\n")
        print("Applies to {} file(s):".format(len(license.files)))
        for f in license.files:
            print("  {}".format(f))
        print("\n")
