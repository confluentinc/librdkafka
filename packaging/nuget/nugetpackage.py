#!/usr/bin/env python3
#
# Create NuGet package
#

import os
import tempfile
import shutil
import subprocess
from packaging import Package, Mapping


class NugetPackage (Package):
    """ All platforms, archs, et.al, are bundled into one set of
        NuGet output packages: "main", redist and symbols """

    # See .semamphore/semaphore.yml for where these are built.
    mappings = [
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/include/librdkafka/rdkafka.h',
                'build/native/include/librdkafka/rdkafka.h'),
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/include/librdkafka/rdkafkacpp.h',
                'build/native/include/librdkafka/rdkafkacpp.h'),
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/include/librdkafka/rdkafka_mock.h',
                'build/native/include/librdkafka/rdkafka_mock.h'),

        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/share/doc/librdkafka/README.md',
                'README.md'),
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/share/doc/librdkafka/CONFIGURATION.md',
                'CONFIGURATION.md'),
        Mapping({'arch': 'x64',
                 'plat': 'osx',
                 'lnk': 'all'},
                'librdkafka.tgz',
                './usr/local/share/doc/librdkafka/LICENSES.txt',
                'LICENSES.txt'),

        # OSX x64
        Mapping({'arch': 'x64',
                 'plat': 'osx'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.dylib',
                'runtimes/osx-x64/native/librdkafka.dylib'),
        # OSX arm64
        Mapping({'arch': 'arm64',
                 'plat': 'osx'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.1.dylib',
                'runtimes/osx-arm64/native/librdkafka.dylib'),

        # Linux glibc centos6 x64 with GSSAPI
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'dist': 'centos6',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.so.1',
                'runtimes/linux-x64/native/librdkafka.so'),
        # Linux glibc centos6 x64 without GSSAPI (no external deps)
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'dist': 'centos6',
                 'lnk': 'all'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.so.1',
                'runtimes/linux-x64/native/centos6-librdkafka.so'),
        # Linux glibc centos7 x64 with GSSAPI
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'dist': 'centos7',
                 'lnk': 'std'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.so.1',
                'runtimes/linux-x64/native/centos7-librdkafka.so'),
        # Linux glibc centos7 arm64 without GSSAPI (no external deps)
        Mapping({'arch': 'arm64',
                 'plat': 'linux',
                 'dist': 'centos7',
                 'lnk': 'all'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.so.1',
                'runtimes/linux-arm64/native/librdkafka.so'),

        # Linux musl alpine x64 without GSSAPI (no external deps)
        Mapping({'arch': 'x64',
                 'plat': 'linux',
                 'dist': 'alpine',
                 'lnk': 'all'},
                'librdkafka.tgz',
                './usr/local/lib/librdkafka.so.1',
                'runtimes/linux-x64/native/alpine-librdkafka.so')

    ]

    def __init__(self, version, arts):
        if version.startswith('v'):
            version = version[1:]  # Strip v prefix
        super(NugetPackage, self).__init__(version, arts)

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

        # Apply mappings and extract files
        self.apply_mappings()

        print('Tree extracted to %s' % self.stpath)

        # After creating a bare-bone nupkg layout containing the artifacts
        # and some spec and props files, call the 'nuget' utility to
        # make a proper nupkg of it (with all the metadata files).
        subprocess.check_call("./nuget.sh pack %s -BasePath '%s' -NonInteractive" %  # noqa: E501
                              (os.path.join(self.stpath,
                                            'librdkafka.redist.nuspec'),
                               self.stpath), shell=True)

        return 'librdkafka.redist.%s.nupkg' % vless_version
