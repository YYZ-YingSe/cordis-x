# Explicit OS prerequisites. Run in PowerShell; installers can request elevation.
$ErrorActionPreference = 'Stop'
if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
    throw 'Install App Installer (winget) from Microsoft, then rerun this script.'
}
function Install-Package([string] $Id, [string[]] $Extra = @()) {
    & winget install --id $Id --exact --source winget --accept-package-agreements --accept-source-agreements @Extra
    # APPINSTALLER_CLI_ERROR_UPDATE_NOT_APPLICABLE means the package is current.
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne -1978335189) {
        throw "winget failed installing $Id (exit $LASTEXITCODE)"
    }
}
Install-Package 'Python.Python.3.12'
Install-Package 'Git.Git'
Install-Package 'Microsoft.VisualStudio.2022.BuildTools' @('--override', '--wait --passive --norestart --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended')
Write-Host 'Open a new PowerShell, then run: py -3.12 tools/setup/setup.py --prefix <tool-directory>'
