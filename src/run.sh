#!/bin/bash

# Set permissions for the SSH key
chmod 600 /root/.ssh/id_rsa

# Add the Git host to the list of known hosts
ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

# Clone the repository using the deploy key
git clone -b hepromark/asset-kubernetes2 git@github.com:WATonomous/infra-config.git

# Install libraries
pip install -r requirements.txt

python3 main.py
