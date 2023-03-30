# Download the CoApp tools.
$msiPath = "$($env:USERPROFILE)\\CoApp.Tools.Powershell.msi"

(New-Object Net.WebClient).DownloadFile('https://github.com/coapp/coapp.github.io/blob/master/files/CoApp.Tools.Powershell.msi', $msiPath)

# Install the CoApp tools from the downloaded .msi.
Start-Process -FilePath msiexec -ArgumentList /i, $msiPath, /quiet -Wait

# Make the tools available for later PS scripts to use.
$env:PSModulePath = $env:PSModulePath + ';C:\\Program Files (x86)\\Outercurve Foundation\\Modules'

# Get-module -ListAvailable

Get-ChildItem C:\\ -include *CoApp* -recurse -ErrorAction Continue 2>&1|%{
  if($_ -is [System.Management.Automation.ErrorRecord]){
    # if this is an error, print the exception message directly to the screen
    Write-Host $_.Exception.Message -ForegroundColor Red
  }
  else {
    # otherwise, just output the input object as-is
    $_
  }
}

Import-Module CoApp
