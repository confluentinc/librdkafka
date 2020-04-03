param([String]$OpenSSLVersion="1.1.1f")

$ErrorActionPreference = "stop"

$FileVersion = $OpenSSLVersion -replace "\.", "_"

function Install-OpenSSL {
    param([String]$Arch)

    $instDir="C:\OpenSSL-$Arch-$OpenSSLVersion"
    $destDir="C:\OpenSSL-$Arch"

    if (!(Test-Path($instDir))) {
        $exeFull = "${Arch}OpenSSL-${FileVersion}.exe"
        $url = "https://slproweb.com/download/$exeFull"
        $exePath = "$($env:USERPROFILE)\$exeFull"

        Write-Host "Downloading and installing OpenSSL $OpenSSLVersion $Arch from $url" -ForegroundColor Cyan
        (New-Object Net.WebClient).DownloadFile($url, $exePath)

        Write-Host "Installing to $instDir..."
        cmd /c start /wait $exePath /silent /verysilent /sp- /suppressmsgboxes /DIR=$instDir
        Write-Host "Installed" -ForegroundColor Green
    } else {
        echo "$instDir already exists: not downloading"
    }

    echo "Copying $instDir to $destDir"
    Remove-Item -Recurse -Force -Path $destDir
    New-Item -Path $destDir -ItemType "directory"
    Get-ChildItem -Path $instDir | Copy-Item -Destination $destDir -Recurse -Container
}

Install-OpenSSL "Win32"
Install-OpenSSL "Win64"

