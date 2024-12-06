import itertools
import json
import logging
import os
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile

from watcloud_utils.typer import app
from git import GitCommandError, Repo

from .watcloud_uri import WATcloudURI


flatten = itertools.chain.from_iterable


def clone_repos(repo_config, workspace_dir):
    for config in repo_config["repos"]:
        if config["type"] == "local":
            repo = Repo(config["path"])
            logging.info(f"Using existing repo at {repo.working_dir}")
        elif config["type"] == "git+https":
            repo_url = config["url"]
            repo_path = workspace_dir / repo_url

            if repo_path.exists():
                logging.debug(
                    f"Path {repo_path} already exists. Pulling latest changes."
                )
                repo = Repo(repo_path)
                repo.remote().pull()
                logging.info(f"Pulled latest changes to {repo.working_dir}")
            else:
                logging.debug(f"Path {repo_path} does not exist. Cloning repo.")
                repo = Repo.clone_from(repo_url, repo_path)
                logging.info(f"Cloned {repo_url} to {repo.working_dir}")
        elif config["type"] == "git+ssh":
            repo_url = config["url"]
            deploy_key_path = config["deploy_key_path"]

            # Temporary file is required to handle ssh key permissions.
            # NamedTemporaryFile is always created with mode 0600:
            # https://stackoverflow.com/a/10541972
            with NamedTemporaryFile() as deploy_key_file:
                logging.debug(f"Copying deploy key from {deploy_key_path} to {deploy_key_file.name}")
                deploy_key_file.write(Path(deploy_key_path).read_bytes())
                deploy_key_file.flush()
        
                repo_path = workspace_dir / repo_url

                if repo_path.exists():
                    logging.debug(f"Path {repo_path} already exists. Pulling latest changes.")
                    repo = Repo(repo_path)
                    repo.remote().pull(env={"GIT_SSH_COMMAND": f"ssh -i {deploy_key_file.name}"})
                    logging.info(f"Pulled latest changes to {repo.working_dir}")
                else:
                    logging.debug(f"Path {repo_path} does not exist. Cloning repo.")
                    repo = Repo.clone_from(repo_url, repo_path, env={"GIT_SSH_COMMAND": f"ssh -i {deploy_key_file.name}"})
                    logging.info(f"Cloned {repo_url} to {repo.working_dir}")
        else:
            raise ValueError(f"Unsupported repo type '{config['type']}'")

        yield repo


@app.command()
def get_raw_watcloud_uris(repo_path: Path):
    repo = Repo(repo_path)

    # -h suppresses filename output
    # --only-matching returns only the matched text
    try:
        out = repo.git.execute(
            ["git", "grep", "--only-matching", "-h", "watcloud://[^\"' ]*"]
            + [r.name for r in repo.refs]
        )
    except GitCommandError as e:
        # when `git grep` doesn't find any matches, it throws a GitCommandError with status 1
        if e.status == 1:
            logging.debug(f"{repo.working_dir} does not contain any WATcloud URIs")
            out = ""
        else:
            raise

    uris = set(u.strip() for u in out.splitlines() if u.strip())

    return uris


@app.command()
def get_watcloud_uris(repo_path: Path):
    raw_uris = get_raw_watcloud_uris(repo_path)

    for uri in raw_uris:
        try:
            yield WATcloudURI(uri)
        except ValueError as e:
            logging.debug(f"Skipping invalid WATcloud URI '{uri}': {e}")


if __name__ == "__main__":
    app()
