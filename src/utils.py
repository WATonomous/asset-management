import itertools
import json
import logging
import os
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile

import boto3
import typer
from git import GitCommandError, Repo

from watcloud_uri import WATcloudURI

AGENT_CONFIG = json.loads(os.environ["AGENT_CONFIG"])
WORKSPACE = Path("/tmp/workspace")
WORKSPACE.mkdir(exist_ok=True, parents=True)

flatten = itertools.chain.from_iterable


def typer_result_callback(ret, *args, **kwargs):
    # This is useful when the return value is a generator
    if hasattr(ret, "__iter__"):
        ret = list(ret)

    print(ret)


app = typer.Typer(result_callback=typer_result_callback)


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@app.callback()
def callback(log_level: LogLevel = LogLevel.INFO):
    logging.basicConfig(
        level=log_level.value,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )


@app.command()
def clone_repos():
    for repo_url, repo_config in AGENT_CONFIG["repos"].items():
        if "path" in repo_config:
            repo = Repo(repo_config["path"])
            logging.info(f"Using existing repo at {repo.working_dir}")
        elif "deploy_key_path" in repo_config:
            # Temporary file is required to handle ssh key permissions.
            # NamedTemporaryFile is always created with mode 0600:
            # https://stackoverflow.com/a/10541972
            with NamedTemporaryFile() as deploy_key_file:
                logging.debug(
                    f"Copying deploy key from {repo_config['deploy_key_path']} to {deploy_key_file.name}"
                )
                deploy_key_file.write(Path(repo_config["deploy_key_path"]).read_bytes())
                deploy_key_file.flush()

                repo_path = WORKSPACE / repo_url

                if repo_path.exists():
                    logging.debug(
                        f"Path {repo_path} already exists. Pulling latest changes."
                    )
                    repo = Repo(repo_path)
                    repo.remote().pull(
                        env={"GIT_SSH_COMMAND": f"ssh -i {deploy_key_file.name}"}
                    )
                    logging.info(f"Pulled latest changes to {repo.working_dir}")
                else:
                    logging.debug(f"Path {repo_path} does not exist. Cloning repo.")
                    repo = Repo.clone_from(
                        repo_url,
                        repo_path,
                        env={"GIT_SSH_COMMAND": f"ssh -i {deploy_key_file.name}"},
                    )
                    logging.info(f"Cloned {repo_url} to {repo.working_dir}")

        yield repo


@app.command()
def get_raw_watcloud_uris(repo_path: Path):
    repo = Repo(repo_path)

    # -h suppresses filename output
    # --only-matching returns only the matched text
    try:
        out = repo.git.execute(
            ["git", "grep", "--only-matching", "-h", "watcloud://[^\"' ]*"]
            + [r.name for r in repo.remote().refs]
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


@app.command()
def get_bucket(bucket_name: str):
    return boto3.resource(
        "s3",
        endpoint_url=AGENT_CONFIG["buckets"][bucket_name]["endpoint"],
        aws_access_key_id=os.environ[
            AGENT_CONFIG["buckets"][bucket_name]["access_key_env_var"]
        ],
        aws_secret_access_key=os.environ[
            AGENT_CONFIG["buckets"][bucket_name]["secret_key_env_var"]
        ],
    ).Bucket(AGENT_CONFIG["buckets"][bucket_name]["bucket"])


if __name__ == "__main__":
    app()
