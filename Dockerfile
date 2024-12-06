FROM python:3.11-bookworm

# Install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt --break-system-packages

# Copy files into container
WORKDIR /app
COPY /src /app

# Add github.com to known hosts
RUN mkdir /root/.ssh/ && ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

CMD ["python", "-m", "src.main", "run-agent"]
