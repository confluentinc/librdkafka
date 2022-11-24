#!/usr/bin/env python3
#
# Create self-contained static-library tar-ball package
#

import os
import tempfile
import shutil
import subprocess
from fnmatch import fnmatch
from packaging import Package, MissingArtifactError, unquote
from zfile import zfile


class StaticPackage (Package):
    """ Create a tar-ball with self-contained static libraries.
        These are later imported into confluent-kafka-go. """

    # Only match statically linked artifacts
    match = {'lnk': 'static'}

    def __init__(self, version, arts):
        super(StaticPackage, self).__init__(version, arts, "static")

    def cleanup(self):
        if os.path.isdir(self.stpath):
            shutil.rmtree(self.stpath)

    def build(self, buildtype):
        """ Build single package for all artifacts. """

        self.stpath = tempfile.mkdtemp(prefix="out-", dir=".")

        mappings = [
            # rdkafka.h
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './include/librdkafka/rdkafka.h',
             'rdkafka.h'],

            # LICENSES.txt
            [{'arch': 'x64',
              'plat': 'osx',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './share/doc/librdkafka/LICENSES.txt',
             'LICENSES.txt'],

            # glibc linux static lib and pkg-config file
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './lib/librdkafka-static.a',
             'librdkafka_glibc_linux.a'],
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './lib/pkgconfig/rdkafka-static.pc',
             'librdkafka_glibc_linux.pc'],

            # musl linux static lib and pkg-config file
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'alpine-librdkafka.tgz'},
             'librdkafka-static.a',
             'librdkafka_musl_linux.a'],
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'alpine-librdkafka.tgz'},
             'rdkafka-static.pc',
             'librdkafka_musl_linux.pc'],

            # glibc linux arm64 static lib and pkg-config file
            [{'arch': 'arm64',
              'plat': 'linux',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/librdkafka-static.a',
             'librdkafka_glibc_linux_arm64.a'],
            [{'arch': 'arm64',
              'plat': 'linux',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/pkgconfig/rdkafka-static.pc',
             'librdkafka_glibc_linux_arm64.pc'],

            # musl linux arm64 static lib and pkg-config file
            [{'arch': 'arm64',
              'plat': 'linux',
              'fname_glob': 'alpine-librdkafka.tgz'},
             'librdkafka-static.a',
             'librdkafka_musl_linux_arm64.a'],
            [{'arch': 'arm64',
              'plat': 'linux',
              'fname_glob': 'alpine-librdkafka.tgz'},
             'rdkafka-static.pc',
             'librdkafka_musl_linux_arm64.pc'],

            # osx x64 static lib and pkg-config file
            [{'arch': 'x64', 'plat': 'osx',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './lib/librdkafka-static.a',
             'librdkafka_darwin_amd64.a'],
            [{'arch': 'x64', 'plat': 'osx',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './lib/pkgconfig/rdkafka-static.pc',
             'librdkafka_darwin_amd64.pc'],

            # osx arm64 static lib and pkg-config file
            [{'arch': 'arm64', 'plat': 'osx',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/librdkafka-static.a',
             'librdkafka_darwin_arm64.a'],
            [{'arch': 'arm64', 'plat': 'osx',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/pkgconfig/rdkafka-static.pc',
             'librdkafka_darwin_arm64.pc'],

            # win static lib and pkg-config file (mingw)
            [{'arch': 'x64', 'plat': 'win',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
                './lib/librdkafka-static.a', 'librdkafka_windows.a'],
            [{'arch': 'x64', 'plat': 'win',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
                './lib/pkgconfig/rdkafka-static.pc', 'librdkafka_windows.pc'],
        ]

        for m in mappings:
            attributes = m[0].copy()
            attributes.update(self.match)
            fname_glob = attributes['fname_glob']
            del attributes['fname_glob']
            fname_excludes = []
            if 'fname_excludes' in attributes:
                fname_excludes = attributes['fname_excludes']
                del attributes['fname_excludes']

            artifact = None
            for a in self.arts.artifacts:
                found = True

                for attr in attributes:
                    if attr not in a.info or a.info[attr] != attributes[attr]:
                        found = False
                        break

                if not fnmatch(a.fname, fname_glob):
                    found = False

                for exclude in fname_excludes:
                    if exclude in a.fname:
                        found = False
                        break

                if found:
                    artifact = a
                    break

            if artifact is None:
                raise MissingArtifactError(
                    'unable to find artifact with tags %s matching "%s"' %
                    (str(attributes), fname_glob))

            outf = os.path.join(self.stpath, m[2])
            member = m[1]
            try:
                zfile.ZFile.extract(artifact.lpath, member, outf)
            except KeyError as e:
                raise Exception(
                    'file not found in archive %s: %s. Files in archive are: %s' %  # noqa: E501
                    (artifact.lpath, e, zfile.ZFile(
                        artifact.lpath).getnames()))

        print('Tree extracted to %s' % self.stpath)

        # After creating a bare-bone layout, create a tarball.
        outname = "librdkafka-static-bundle-%s.tgz" % self.version
        print('Writing to %s' % outname)
        subprocess.check_call("(cd %s && tar cvzf ../%s .)" %
                              (self.stpath, outname),
                              shell=True)

        return outname

    def verify(self, path):
        """ Verify package """
        expect = [
            "./rdkafka.h",
            "./LICENSES.txt",
            "./librdkafka_glibc_linux.a",
            "./librdkafka_glibc_linux.pc",
            "./librdkafka_glibc_linux_arm64.a",
            "./librdkafka_glibc_linux_arm64.pc",
            "./librdkafka_musl_linux.a",
            "./librdkafka_musl_linux.pc",
            "./librdkafka_musl_linux_arm64.a",
            "./librdkafka_musl_linux_arm64.pc",
            "./librdkafka_darwin_amd64.a",
            "./librdkafka_darwin_arm64.a",
            "./librdkafka_darwin_amd64.pc",
            "./librdkafka_darwin_arm64.pc",
            "./librdkafka_windows.a",
            "./librdkafka_windows.pc"]

        missing = list()
        with zfile.ZFile(path, 'r') as zf:
            print('Verifying %s:' % path)

            # Zipfiles may url-encode filenames, unquote them before matching.
            pkgd = [unquote(x) for x in zf.getnames()]
            missing = [x for x in expect if x not in pkgd]

        if len(missing) > 0:
            print(
                'Missing files in package %s:\n%s' %
                (path, '\n'.join(missing)))
            return False
        else:
            print('OK - %d expected files found' % len(expect))
            return True
