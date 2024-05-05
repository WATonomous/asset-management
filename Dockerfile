FROM python:3.11-bookworm

# Copy files into container
WORKDIR /app
COPY /src/main.py /app
COPY /src/run.sh /app

# Copy the private SSH key from github provisioning to the container
# RUN mkdir /root/.ssh/
# RUN echo -e "${SSH_DEPLOY_KEY}" > /root/.ssh/id_rsa
# RUN cat /root/.ssh/id_rsa
# COPY id_rsa /root/.ssh/id_rsa

# Set permissions for the SSH key
# RUN chmod 600 /root/.ssh/id_rsa

# Add the Git host to the list of known hosts
# RUN ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

# Clone the repository using the deploy key
# RUN git clone git@github.com:WATonomous/infra-config.git

# Install s3cmd
# RUN pip install s3cmd

# Run asset-management script
RUN chmod +x run.sh
CMD ["./run.sh"]
# CMD ["echo $PATH"]
