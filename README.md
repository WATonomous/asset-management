# WATcloud Asset Management System

This repo contains the asset management system for WATcloud.
Currently, only the agent implementation is in this repo.
Additional components, including the SDK, the S3 bucket configuration, and deployment code, reside in the internal monorepo [infra-config](https://github.com/WATonomous/infra-config).

## Useful Links

- [Asset Manager Frontend](https://cloud.watonomous.ca/docs/utilities/assets)

## Getting Started (Agent Development)

Run the following commands to start the development environment:

```bash
docker compose up -d --build
```

Enter the container:

```bash
docker compose exec agent bash
```

Start the agent:

```bash
python -m src.main run-agent
```

By default, the agent will interface with the local minio server and look for WATcloud URIs in a dummy git repository. These are configurable in `docker-compose.yml` and via environment variables in the container.
