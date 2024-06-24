#!/bin/bash

# Set permissions for the SSH key
chmod 600 /root/.ssh/id_rsa

# Add the Git host to the list of known hosts
ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

# Clone the repository using the deploy key
git clone -b master git@github.com:WATonomous/infra-config.git

# Install libraries
pip install -r requirements.txt

# Get list of branches
cd infra-config
BRANCHES=$(git ls-remote --heads origin | sed 's?.*refs/heads/??')

# Loop through each branch
for BRANCH in $BRANCHES; do
    echo "Managing Assets for: $BRANCH"
    git checkout $BRANCH
    git pull origin $BRANCH

    cd ..
    python3 main.py
    cd infra-config
done
