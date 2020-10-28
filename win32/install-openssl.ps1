$OpenSSLVersion = "1_1_1g"
$OpenSSLExe = "OpenSSL-$OpenSSLVersion.exe"
$OpenSSLUriBase = "https://slproweb.com/download"

if (!(Test-Path("C:\OpenSSL-Win32"))) {
   $instDir = "C:\OpenSSL-Win32"
   $exeFull = "Win32$OpenSSLExe"
   $exePath = "$($env:USERPROFILE)\$exeFull"
   $uri     = "$OpenSSLUriBase/$exeFull"

   Write-Host "Downloading and installing OpenSSL v$OpenSSLVersion 32-bit via $uri ..." -ForegroundColor Cyan
   (New-Object Net.WebClient).DownloadFile($uri, $exePath)

   Write-Host "Installing to $instDir..."
   Start-Process -FilePath $exePath -ArgumentList "/silent /verysilent /sp- /suppressmsgboxes /DIR=`"$instDir`""
   Write-Host "Installed" -ForegroundColor Green
} else {
   Write-Output "OpenSSL-Win32 already exists: not downloading"
}


if (!(Test-Path("C:\OpenSSL-Win64"))) {
   $instDir = "C:\OpenSSL-Win64"
   $exeFull = "Win64$OpenSSLExe"
   $exePath = "$($env:USERPROFILE)\$exeFull"
   $uri     = "$OpenSSLUriBase/$exeFull"

   Write-Host "Downloading and installing OpenSSL v$OpenSSLVersion 64-bit via $uri ..." -ForegroundColor Cyan
   (New-Object Net.WebClient).DownloadFile($uri, $exePath)

   Write-Host "Installing to $instDir..."
   Start-Process -FilePath $exePath -ArgumentList "/silent /verysilent /sp- /suppressmsgboxes /DIR=`"$instDir`""
   Write-Host "Installed" -ForegroundColor Green
} else {
   Write-Output "OpenSSL-Win64 already exists: not downloading"
}
