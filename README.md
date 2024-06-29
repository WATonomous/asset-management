# WATcloud Asset Management System

This repo contains the asset management system for WATcloud.
Currently, only the agent implementation is in this repo.
Additional components, including the SDK, the S3 bucket configuration, and deployment code, reside in the internal monorepo [infra-config](https://github.com/WATonomous/infra-config).

## Useful Links

- [Asset Manager Frontend](https://cloud.watonomous.ca/docs/utilities/assets)

## Getting Started (Agent Development)

Copy the `.env.example` file to `.env` and fill in the necessary information.

Create `./tmp/deploy-keys` directory and place the required deploy keys in the directory. The list of deploy keys can be configured in `docker-compose.yml`.

Run the following commands to start the development environment:

```bash
docker compose up -d --build
```

Enter the container:

```bash
docker compose exec agent bash
```
