# Install (if necessary) and set up msys2.
$ErrorActionPreference = "Stop"

$url="https://github.com/msys2/msys2-installer/releases/download/2024-01-13/msys2-base-x86_64-20240113.sfx.exe"
$sha256="dba7e6d27e6a9ab850f502da44f6bfcd16d4d7b175fc2b25bee37207335cb12f"
$cacheKey = "msys2-$sha256-$Env:CACHE_TAG"
$librdkafkaPath = Get-Location;

try {
    cache restore $cacheKey
} catch {
    echo "cache command not found"
}
if (!(Test-Path -Path "c:\msys64\usr\bin\bash.exe")) {
    echo "Downloading and installing msys2 to c:\msys64"

    (New-Object System.Net.WebClient).DownloadFile($url, './msys2-installer.exe')

    # Verify checksum
    (Get-FileHash -Algorithm "SHA256" .\msys2-installer.exe).hash -eq $sha256

    # Install msys2
    .\msys2-installer.exe -y -oc:\

    Remove-Item msys2-installer.exe

    # Set up msys2 the first time
    echo "Setting up msys"
    c:\msys64\usr\bin\bash -lc ' '

    # Update packages
    echo "Updating msys2 packages"
    c:\msys64\usr\bin\bash -lc "pacman --noconfirm -Syuu --overwrite '*'"
    # Update pacman"
    c:\msys64\usr\bin\bash -lc "pacman -Sy --noconfirm pacman"
    # Install needed packages"
    c:\msys64\usr\bin\bash -lc "pacman --sync --noconfirm --needed mingw-w64-x86_64-gcc mingw-w64-x86_64-make mingw-w64-x86_64-cmake mingw-w64-x86_64-openssl mingw-w64-x86_64-lz4 mingw-w64-x86_64-zstd"

    try {
        powershell -command "cd $librdkafkaPath; cache store $cacheKey c:\msys64"
    } catch {
        echo "cache command not found"
    }

} else {
    echo "Using previously installed msys2"
}
