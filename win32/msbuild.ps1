param(
    [string]$config='Release',
    [string]$platform='x64',
    [string]$toolset='v142'
)

$msbuild = (& "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -prerelease -products * -requires Microsoft.Component.MSBuild -find MSBuild\**\Bin\MSBuild.exe)

echo "Using msbuild $msbuild"

echo "Cleaning $config $platform $toolset"
& $msbuild win32\librdkafka.sln /p:Configuration=$config /p:Platform=$platform /p:PlatformToolset=$toolset /target:Clean

echo "Building $config $platform $toolset"
& $msbuild win32\librdkafka.sln /p:Configuration=$config /p:Platform=$platform /p:PlatformToolset=$toolset
if ($LASTEXITCODE -ne 0) {
    Write-Error "msbuild failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

cd tests
$env:CI = "true";
$env:TEST_CONSUMER_GROUP_PROTOCOL = "classic";
..\win32\outdir\$toolset\$platform\Release\tests.exe -l -Q
if ($LASTEXITCODE -ne 0) {
    Write-Error "Classic test failed with exit code $LASTEXITCODE"
    cd ..
    exit $LASTEXITCODE
}
$env:TEST_CONSUMER_GROUP_PROTOCOL = "consumer";
..\win32\outdir\$toolset\$platform\Release\tests.exe -l -Q
if ($LASTEXITCODE -ne 0) {
    Write-Error "Consumer test failed with exit code $LASTEXITCODE"
    cd ..
    exit $LASTEXITCODE
}
cd ..
