<#
.SYNOPSIS

   Create zip package


.DESCRIPTION

   A full build must be completed, to populate output directories, before

   running this script.

   Use build.bat to build

#>

param(
    [string]$config='Release',
    [string]$platform='x64',
    [string]$toolset='v142',
    [string]$version='0.0.0'
)

$msbuild = (& "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -prerelease -products * -requires Microsoft.Component.MSBuild -find MSBuild\**\Bin\MSBuild.exe)

echo "Packaging $config $platform $toolset"

$bindir = "build\native\bin\${toolset}\${platform}\$config"
$libdir = "build\native\lib\${toolset}\${platform}\$config"
$srcdir = "win32\outdir\${toolset}\${platform}\$config"

New-Item -Path $bindir -ItemType Directory
New-Item -Path $libdir -ItemType Directory

$platformpart = ""
if ("x64" -eq $platform) {
   $platformpart = "-${platform}"
}

Copy-Item "${srcdir}\librdkafka.dll","${srcdir}\librdkafkacpp.dll",
"${srcdir}\libcrypto-3${platformpart}.dll","${srcdir}\libssl-3${platformpart}.dll",
"${srcdir}\zlib1.dll","${srcdir}\zstd.dll","${srcdir}\libcurl.dll" -Destination $bindir

# Ship the rdkafka-aws-sts shim DLL. AWS SDK is statically linked inside the
# shim (built via the x{64,86}-windows-static vcpkg triplet), so there are no
# aws-cpp-sdk-*.dll files to ship alongside it.
Copy-Item "${srcdir}\rdkafka-aws-sts.dll" -Destination $bindir

Copy-Item "${srcdir}\librdkafka.lib","${srcdir}\librdkafkacpp.lib" -Destination $libdir

7z.exe a "artifacts\librdkafka.redist.zip" "build"
