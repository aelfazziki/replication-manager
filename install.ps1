# Configuration
$REPO_URL = "https://github.com/yourusername/replication-manager.git"
$INSTALL_DIR = "C:\Program Files\ReplicationManager"

# Check and install Git
if (-not (Test-Path "git.exe")) {
    Write-Host "Downloading Git..."
    $GIT_INSTALLER = "$env:TEMP\git-install.exe"
    Invoke-WebRequest -Uri "https://github.com/git-for-windows/git/releases/download/v2.41.0.windows.3/Git-2.41.0.3-64-bit.exe" -OutFile $GIT_INSTALLER
    Start-Process -Wait -FilePath $GIT_INSTALLER -ArgumentList "/VERYSILENT /NORESTART"
    $env:Path += ";$env:ProgramFiles\Git\cmd"
}

# Create installation directory
New-Item -Path $INSTALL_DIR -ItemType Directory -Force
Set-Location $INSTALL_DIR

# Clone source code
Write-Host "Cloning source code from $REPO_URL..."
git clone $REPO_URL .
if (-not $?) {
    Write-Host "Failed to clone repository. Exiting."
    exit 1
}

# Rest of the installation process...
python -m venv venv
.\venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# ... (rest of previous Windows installation steps)