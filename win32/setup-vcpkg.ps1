# Set up vcpkg and install required packages.
$version = "2026.07.29"
# Hash everything that decides what vcpkg installs: the manifest, its
# configuration and the overlay ports.
$vpkgInputs = @(Get-Item ".\librdkafka\vcpkg.json", ".\librdkafka\vcpkg-configuration.json") +
              @(Get-ChildItem ".\librdkafka\win32\vcpkg-overlay-ports" -Recurse -File | Sort-Object FullName)
$vpkgHashes = ($vpkgInputs | Get-FileHash).Hash -join ""
$vpkgHash = (Get-FileHash -InputStream ([IO.MemoryStream]::new([Text.Encoding]::UTF8.GetBytes($vpkgHashes)))).Hash
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
    # Install required packages.
    ..\vcpkg\vcpkg --feature-flags=versions install --triplet $Env:triplet
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

