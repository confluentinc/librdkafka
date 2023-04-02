<#
.SYNOPSIS

   Create NuGet package


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

& $msbuild win32\librdkafka.vcxproj /p:Configuration=$config /p:Platform=$platform /p:PlatformToolset=$toolset /p:RestorePackagesConfig=true /t:restore
& $msbuild win32\librdkafka.vcxproj /p:Configuration=$config /p:Platform=$platform /p:PlatformToolset=$toolset /p:ProductVersion=$version /t:pack

