<#
.SYNOPSIS

   Create NuGet package using CoApp


.DESCRIPTION

   A full build must be completed, to populate output directories, before

   running this script.

   Use build.bat to build


   Requires CoApp
#>

param(
    [string]$version='0.0.0',
    [string]$destdir='.\artifacts'
)

$autopkgFile = "win32/librdkafka.autopkg"
cat ($autopkgFile + ".template") | % { $_ -replace "@version", $version } > $autopkgFile

Write-NuGetPackage $autopkgFile

Move-Item -Path .\*.nupkg -Destination $destdir

