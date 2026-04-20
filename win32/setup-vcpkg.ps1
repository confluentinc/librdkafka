# Set up vcpkg and install required packages.
$version = "37dd44f23f51041f480440e99b45e5706f47765d"
$vpkgHash = (Get-FileHash ".\librdkafka\vcpkg.json").Hash + `
            (Get-FileHash ".\librdkafka\win32\rdkafka-aws-sts\vcpkg.json").Hash
$cacheKey = "vcpkg-$version-$Env:triplet-$vpkgHash-$Env:CACHE_TAG"
$librdkafkaPath = ".\librdkafka";

try {
    cache restore $cacheKey
} catch {
    echo "cache command not found"
}
if (!(Test-Path -Path vcpkg/.git)) {
    git clone https://github.com/Microsoft/vcpkg.git
    cd vcpkg
    git checkout $version
    .\bootstrap-vcpkg.bat
    cd ..
    cd librdkafka
    ..\vcpkg\vcpkg integrate install
    # Install librdkafka's own deps (dynamic triplet).
    ..\vcpkg\vcpkg --feature-flags=versions install --triplet $Env:triplet
    # Install rdkafka-aws-sts shim deps (static triplet) from its own manifest.
    # Static triplet lets the shim DLL be fully /MT with no AWS SDK DLL
    # dependency and no mixed-CRT inside librdkafka.dll.
    ..\vcpkg\vcpkg --feature-flags=versions install `
        --triplet "$Env:triplet-static" `
        --x-manifest-root=win32\rdkafka-aws-sts
    cd ..
    try {
        cache store $cacheKey .\vcpkg
    } catch {
        echo "cache command not found"
    }
} else {
    cd librdkafka
    ..\vcpkg\vcpkg integrate install
    cd ..
    echo "Using previously installed vcpkg"
}

