#!/bin/bash

# Configuration
REPO_URL="https://github.com/yourusername/replication-manager.git"
INSTALL_DIR="/opt/replication-manager"

# Check prerequisites
if ! command -v git &> /dev/null; then
    echo "Installing Git..."
    sudo apt-get install -y git || sudo dnf install -y git
fi

# Create installation directory
sudo mkdir -p $INSTALL_DIR
sudo chown $(whoami): $INSTALL_DIR
cd $INSTALL_DIR

# Clone source code
echo "Cloning source code from $REPO_URL..."
git clone $REPO_URL .
if [ $? -ne 0 ]; then
    echo "Failed to clone repository. Exiting."
    exit 1
fi

# Check for Python 3.8+
if ! command -v python3 &> /dev/null; then
    echo "Installing Python 3.8..."
    sudo apt-get install -y python3.8 python3.8-venv || \
    sudo dnf install -y python38
fi

# Rest of the installation process...
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# ... (rest of previous Linux installation steps)