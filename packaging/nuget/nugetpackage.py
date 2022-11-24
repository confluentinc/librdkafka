#!/usr/bin/env python3
#
# Create NuGet package
#

import os
import tempfile
import shutil
import subprocess
from fnmatch import fnmatch
from packaging import Package, MissingArtifactError, magic_mismatch, unquote
from zfile import zfile


class NugetPackage (Package):
    """ All platforms, archs, et.al, are bundled into one set of
        NuGet output packages: "main", redist and symbols """

    def __init__(self, version, arts):
        if version.startswith('v'):
            version = version[1:]  # Strip v prefix
        super(NugetPackage, self).__init__(version, arts, "nuget")

    def cleanup(self):
        if os.path.isdir(self.stpath):
            shutil.rmtree(self.stpath)

    def build(self, buildtype):
        """ Build single NuGet package for all its artifacts. """

        # NuGet removes the prefixing v from the version.
        vless_version = self.kv['version']
        if vless_version[0] == 'v':
            vless_version = vless_version[1:]

        self.stpath = tempfile.mkdtemp(prefix="out-", suffix="-%s" % buildtype,
                                       dir=".")

        self.render('librdkafka.redist.nuspec')
        self.copy_template('librdkafka.redist.targets',
                           destpath=os.path.join('build', 'native'))
        self.copy_template('librdkafka.redist.props',
                           destpath='build')

        # Generate template tokens for artifacts
        for a in self.arts.artifacts:
            if 'bldtype' not in a.info:
                a.info['bldtype'] = 'release'

            a.info['variant'] = '%s-%s-%s' % (a.info.get('plat'),
                                              a.info.get('arch'),
                                              a.info.get('bldtype'))
            if 'toolset' not in a.info:
                a.info['toolset'] = 'v142'

        mappings = [
            [{'arch': 'x64',
              'plat': 'linux',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './include/librdkafka/rdkafka.h',
             'build/native/include/librdkafka/rdkafka.h'],
            [{'arch': 'x64',
              'plat': 'linux',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './include/librdkafka/rdkafkacpp.h',
             'build/native/include/librdkafka/rdkafkacpp.h'],
            [{'arch': 'x64',
              'plat': 'linux',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './include/librdkafka/rdkafka_mock.h',
             'build/native/include/librdkafka/rdkafka_mock.h'],

            [{'arch': 'x64',
              'plat': 'linux',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './share/doc/librdkafka/README.md',
             'README.md'],
            [{'arch': 'x64',
              'plat': 'linux',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './share/doc/librdkafka/CONFIGURATION.md',
             'CONFIGURATION.md'],
            # The above x64-linux gcc job generates a bad LICENSES.txt file,
            # so we use the one from the osx job instead.
            [{'arch': 'x64',
              'plat': 'osx',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './share/doc/librdkafka/LICENSES.txt',
             'LICENSES.txt'],

            # Travis OSX x64 build
            [{'arch': 'x64', 'plat': 'osx',
              'fname_glob': 'librdkafka-clang.tar.gz'},
             './lib/librdkafka.dylib',
             'runtimes/osx-x64/native/librdkafka.dylib'],
            # Travis OSX arm64 build
            [{'arch': 'arm64', 'plat': 'osx',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/librdkafka.1.dylib',
             'runtimes/osx-arm64/native/librdkafka.dylib'],
            # Travis Manylinux build
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'librdkafka-manylinux*x86_64.tgz'},
             './lib/librdkafka.so.1',
             'runtimes/linux-x64/native/centos6-librdkafka.so'],
            # Travis Ubuntu 14.04 build
            [{'arch': 'x64',
              'plat': 'linux',
              'lnk': 'std',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/librdkafka.so.1',
             'runtimes/linux-x64/native/librdkafka.so'],
            # Travis CentOS 7 RPM build
            [{'arch': 'x64',
              'plat': 'linux',
              'fname_glob': 'librdkafka1*el7.x86_64.rpm'},
             './usr/lib64/librdkafka.so.1',
             'runtimes/linux-x64/native/centos7-librdkafka.so'],
            # Travis Alpine build
            [{'arch': 'x64', 'plat': 'linux',
              'fname_glob': 'alpine-librdkafka.tgz'},
             'librdkafka.so.1',
             'runtimes/linux-x64/native/alpine-librdkafka.so'],
            # Travis arm64 Linux build
            [{'arch': 'arm64', 'plat': 'linux',
              'fname_glob': 'librdkafka-gcc.tar.gz'},
             './lib/librdkafka.so.1',
             'runtimes/linux-arm64/native/librdkafka.so'],

            # Common Win runtime
            [{'arch': 'x64', 'plat': 'win', 'fname_glob': 'msvcr140.zip'},
             'vcruntime140.dll',
             'runtimes/win-x64/native/vcruntime140.dll'],
            [{'arch': 'x64', 'plat': 'win', 'fname_glob': 'msvcr140.zip'},
                'msvcp140.dll', 'runtimes/win-x64/native/msvcp140.dll'],
            # matches librdkafka.redist.{VER}.nupkg
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/librdkafka.dll',
             'runtimes/win-x64/native/librdkafka.dll'],
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/librdkafkacpp.dll',
             'runtimes/win-x64/native/librdkafkacpp.dll'],
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/libcrypto-1_1-x64.dll',
             'runtimes/win-x64/native/libcrypto-1_1-x64.dll'],
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/libssl-1_1-x64.dll',
             'runtimes/win-x64/native/libssl-1_1-x64.dll'],
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/zlib1.dll',
             'runtimes/win-x64/native/zlib1.dll'],
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/zstd.dll',
             'runtimes/win-x64/native/zstd.dll'],
            [{'arch': 'x64',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/x64/Release/libcurl.dll',
             'runtimes/win-x64/native/libcurl.dll'],
            # matches librdkafka.{VER}.nupkg
            [{'arch': 'x64', 'plat': 'win', 'fname_glob': 'librdkafka*.nupkg',
              'fname_excludes': ['redist', 'symbols']},
             'build/native/lib/v142/x64/Release/librdkafka.lib',
             'build/native/lib/win/x64/win-x64-Release/v142/librdkafka.lib'],
            [{'arch': 'x64', 'plat': 'win', 'fname_glob': 'librdkafka*.nupkg',
              'fname_excludes': ['redist', 'symbols']},
             'build/native/lib/v142/x64/Release/librdkafkacpp.lib',
             'build/native/lib/win/x64/win-x64-Release/v142/librdkafkacpp.lib'],  # noqa: E501

            [{'arch': 'x86', 'plat': 'win', 'fname_glob': 'msvcr140.zip'},
                'vcruntime140.dll',
             'runtimes/win-x86/native/vcruntime140.dll'],
            [{'arch': 'x86', 'plat': 'win', 'fname_glob': 'msvcr140.zip'},
                'msvcp140.dll', 'runtimes/win-x86/native/msvcp140.dll'],
            # matches librdkafka.redist.{VER}.nupkg
            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/librdkafka.dll',
             'runtimes/win-x86/native/librdkafka.dll'],
            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/librdkafkacpp.dll',
             'runtimes/win-x86/native/librdkafkacpp.dll'],
            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/libcrypto-1_1.dll',
             'runtimes/win-x86/native/libcrypto-1_1.dll'],
            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/libssl-1_1.dll',
             'runtimes/win-x86/native/libssl-1_1.dll'],

            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/zlib1.dll',
             'runtimes/win-x86/native/zlib1.dll'],
            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/zstd.dll',
             'runtimes/win-x86/native/zstd.dll'],
            [{'arch': 'x86',
              'plat': 'win',
              'fname_glob': 'librdkafka.redist*'},
             'build/native/bin/v142/Win32/Release/libcurl.dll',
             'runtimes/win-x86/native/libcurl.dll'],

            # matches librdkafka.{VER}.nupkg
            [{'arch': 'x86', 'plat': 'win', 'fname_glob': 'librdkafka*.nupkg',
              'fname_excludes': ['redist', 'symbols']},
             'build/native/lib/v142/Win32/Release/librdkafka.lib',
             'build/native/lib/win/x86/win-x86-Release/v142/librdkafka.lib'],
            [{'arch': 'x86', 'plat': 'win', 'fname_glob': 'librdkafka*.nupkg',
              'fname_excludes': ['redist', 'symbols']},
             'build/native/lib/v142/Win32/Release/librdkafkacpp.lib',
             'build/native/lib/win/x86/win-x86-Release/v142/librdkafkacpp.lib']
        ]

        for m in mappings:
            attributes = m[0]
            fname_glob = attributes['fname_glob']
            del attributes['fname_glob']
            fname_excludes = []
            if 'fname_excludes' in attributes:
                fname_excludes = attributes['fname_excludes']
                del attributes['fname_excludes']

            outf = os.path.join(self.stpath, m[2])
            member = m[1]

            found = False
            # Try all matching artifacts until we find the wanted file (member)
            for a in self.arts.artifacts:
                attr_match = True
                for attr in attributes:
                    if a.info.get(attr, None) != attributes[attr]:
                        attr_match = False
                        break

                if not attr_match:
                    continue

                if not fnmatch(a.fname, fname_glob):
                    continue

                for exclude in fname_excludes:
                    if exclude in a.fname:
                        continue

                try:
                    zfile.ZFile.extract(a.lpath, member, outf)
                except KeyError:
                    continue
                except Exception as e:
                    raise Exception(
                        'file not found in archive %s: %s. Files in archive are: %s' %  # noqa: E501
                        (a.lpath, e, zfile.ZFile(
                            a.lpath).getnames()))

                # Check that the file type matches.
                if magic_mismatch(outf, a):
                    os.unlink(outf)
                    continue

                found = True
                break

            if not found:
                raise MissingArtifactError(
                    'unable to find artifact with tags %s matching "%s" for file "%s"' %  # noqa: E501
                    (str(attributes), fname_glob, member))

        print('Tree extracted to %s' % self.stpath)

        # After creating a bare-bone nupkg layout containing the artifacts
        # and some spec and props files, call the 'nuget' utility to
        # make a proper nupkg of it (with all the metadata files).
        subprocess.check_call("./nuget.sh pack %s -BasePath '%s' -NonInteractive" %  # noqa: E501
                              (os.path.join(self.stpath,
                                            'librdkafka.redist.nuspec'),
                               self.stpath), shell=True)

        return 'librdkafka.redist.%s.nupkg' % vless_version

    def verify(self, path):
        """ Verify package """
        expect = [
            "librdkafka.redist.nuspec",
            "README.md",
            "CONFIGURATION.md",
            "LICENSES.txt",
            "build/librdkafka.redist.props",
            "build/native/librdkafka.redist.targets",
            "build/native/include/librdkafka/rdkafka.h",
            "build/native/include/librdkafka/rdkafkacpp.h",
            "build/native/include/librdkafka/rdkafka_mock.h",
            "build/native/lib/win/x64/win-x64-Release/v142/librdkafka.lib",
            "build/native/lib/win/x64/win-x64-Release/v142/librdkafkacpp.lib",
            "build/native/lib/win/x86/win-x86-Release/v142/librdkafka.lib",
            "build/native/lib/win/x86/win-x86-Release/v142/librdkafkacpp.lib",
            "runtimes/linux-x64/native/centos7-librdkafka.so",
            "runtimes/linux-x64/native/centos6-librdkafka.so",
            "runtimes/linux-x64/native/alpine-librdkafka.so",
            "runtimes/linux-x64/native/librdkafka.so",
            "runtimes/linux-arm64/native/librdkafka.so",
            "runtimes/osx-x64/native/librdkafka.dylib",
            "runtimes/osx-arm64/native/librdkafka.dylib",
            # win x64
            "runtimes/win-x64/native/librdkafka.dll",
            "runtimes/win-x64/native/librdkafkacpp.dll",
            "runtimes/win-x64/native/vcruntime140.dll",
            "runtimes/win-x64/native/msvcp140.dll",
            "runtimes/win-x64/native/libcrypto-1_1-x64.dll",
            "runtimes/win-x64/native/libssl-1_1-x64.dll",
            "runtimes/win-x64/native/zlib1.dll",
            "runtimes/win-x64/native/zstd.dll",
            "runtimes/win-x64/native/libcurl.dll",
            # win x86
            "runtimes/win-x86/native/librdkafka.dll",
            "runtimes/win-x86/native/librdkafkacpp.dll",
            "runtimes/win-x86/native/vcruntime140.dll",
            "runtimes/win-x86/native/msvcp140.dll",
            "runtimes/win-x86/native/libcrypto-1_1.dll",
            "runtimes/win-x86/native/libssl-1_1.dll",
            "runtimes/win-x86/native/zlib1.dll",
            "runtimes/win-x86/native/zstd.dll",
            "runtimes/win-x86/native/libcurl.dll"]

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

        print('OK - %d expected files found' % len(expect))
        return True
