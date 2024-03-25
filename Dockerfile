FROM python:3.11-bookworm

WORKDIR /app

# Copy the private SSH key from github provisioning to the container
COPY id_rsa /root/.ssh/id_rsa
COPY id_rsa.pub /root/.ssh/id_rsa.pub

# Set permissions for the SSH key
RUN chmod 600 /root/.ssh/id_rsa

# Add the Git host to the list of known hosts
RUN ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

# Clone the repository using the deploy key
RUN git clone git@github.com:WATonomous/asset-manager.git
RUN git clone git@github.com:WATonomous/infra-config.git

# Install s3cmd
RUN pip install s3cmd

# Run asset-management script
RUN python3 asset-manager/
