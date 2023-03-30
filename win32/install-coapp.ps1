# Download the CoApp tools.
$msiPath = "$($env:USERPROFILE)\\CoApp.Tools.Powershell.msi"

(New-Object Net.WebClient).DownloadFile('https://github.com/coapp/coapp.github.io/blob/master/files/CoApp.Tools.Powershell.msi', $msiPath)

# Install the CoApp tools from the downloaded .msi.
Start-Process -FilePath msiexec -ArgumentList /i, $msiPath, /quiet -Wait

# Make the tools available for later PS scripts to use.
$env:PSModulePath = $env:PSModulePath + ';C:\\Program Files (x86)\\Outercurve Foundation\\Modules'
Import-Module CoApp
