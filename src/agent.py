import logging
import os
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from jsonschema import validate

from .utils import clone_repos, flatten, get_watcloud_uris

WORKSPACE_DIR = Path(os.getenv("WORKSPACE_DIR", "/tmp/workspace"))

bucket_schema = {
    "type": "object",
    "properties": {
        "endpoint_url": {"type": ["string", "null"]},
        "bucket_name": {"type": "string"},
        
        # one of
        "access_key_id": {"type": "string"},
        "access_key_id_env_var": {"type": "string"},
        
        # one of
        "secret_key": {"type": "string"},
        "secret_key_env_var": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["endpoint_url", "bucket_name"],
}

bucket_config_schema = {
    "type": "object",
    "properties": {
        "temp": bucket_schema,
        "perm": bucket_schema,
        "off-perm": bucket_schema,
    },
    "additionalProperties": False,
    "required": ["temp", "perm", "off-perm"],
}

repo_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "enum": ["git+ssh", "git+https", "local"]},
    },
    "required": ["type"],
    "allOf": [
        {
            "if": {"properties": {"type": {"const": "git+ssh"}}},
            "then": {
                "properties": {
                    "url": {"type": "string"},
                    "deploy_key_path": {"type": "string"},
                },
                "required": ["url", "deploy_key_path"],
            },
        },
        {
            "if": {"properties": {"type": {"const": "git+https"}}},
            "then": {
                "properties": {"url": {"type": "string"}},
                "required": ["url"],
            },
        },
        {
            "if": {"properties": {"type": {"const": "local"}}},
            "then": {
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    ],
}
repo_config_schema = {
    "type": "object",
    "properties": {
        "repos": {
            "type": "array",
            "items": repo_schema,
        }
    },
    "additionalProperties": False,
    "required": ["repos"],
}


class Agent:
    def __init__(self, bucket_config, repo_config, workspace_dir: str):
        logging.info("Initializing agent")
        validate(bucket_config, schema=bucket_config_schema)
        validate(repo_config, schema=repo_config_schema)

        self.buckets = {
            name: boto3.resource(
                "s3",
                endpoint_url=config["endpoint_url"],
                aws_access_key_id=config["access_key_id"] or os.environ[config["access_key_id_env_var"]],
                aws_secret_access_key=config["secret_key"] or os.environ[config["secret_key_env_var"]],
            ).Bucket(config["bucket_name"])
            for name, config in bucket_config.items()
        }

        self.repo_config = repo_config
        self.workspace_dir = Path(workspace_dir)

    def run(self):
        logging.info(f"Starting agent with workspace dir {self.workspace_dir}")
        self.workspace_dir.mkdir(exist_ok=True, parents=True)

        logging.info(f"Preparing {len(self.repo_config['repos'])} repos")
        repos = list(clone_repos(self.repo_config, self.workspace_dir))

        logging.info(f"Extracting WATcloud URIs from {len(repos)} repo(s)")
        watcloud_uris = list(
            # sorting to ensure consistent order for testing
            sorted(flatten([get_watcloud_uris(repo.working_dir) for repo in repos]))
        )

        logging.info(f"Found {len(watcloud_uris)} WATcloud URIs:")
        for uri in watcloud_uris:
            logging.info(uri)

        desired_perm_objects = set(uri.sha256 for uri in watcloud_uris)

        temp_bucket = self.buckets["temp"]
        perm_bucket = self.buckets["perm"]
        off_perm_bucket = self.buckets["off-perm"]

        temp_objects = set(obj.key for obj in temp_bucket.objects.all())
        perm_objects = set(obj.key for obj in perm_bucket.objects.all())
        off_perm_objects = set(obj.key for obj in off_perm_bucket.objects.all())
        all_objects = temp_objects | perm_objects | off_perm_objects

        logging.info(f"Found {len(temp_objects)} objects in temp bucket")
        logging.info(f"Found {len(perm_objects)} objects in perm bucket")
        logging.info(f"Found {len(off_perm_objects)} objects in off-perm bucket")

        errors = []

        if not desired_perm_objects.issubset(all_objects):
            errors.append(
                ValueError(
                    f"Cannot find the following objects in any bucket: {desired_perm_objects - all_objects}"
                )
            )

        # Objects that need to be copied to perm bucket
        to_perm = desired_perm_objects - perm_objects
        temp_to_perm = to_perm & temp_objects
        off_perm_to_perm = to_perm & off_perm_objects

        # Objects that need to be retired from the perm bucket
        perm_to_off_perm = perm_objects - desired_perm_objects

        # Objects that need to be deleted from the temp bucket (already exists in the perm bucket)
        # We don't exclude objects from off-perm because the object in temp may exipre later than the object in off-perm
        delete_from_temp = desired_perm_objects & temp_objects - temp_to_perm

        logging.info(
            f"{len(desired_perm_objects&perm_objects)}/{len(desired_perm_objects)} objects are already in the perm bucket"
        )
        logging.info(f"Copying {len(temp_to_perm)} object(s) from temp to perm bucket:")
        for obj_key in temp_to_perm:
            logging.info(obj_key)
        logging.info(
            f"Copying {len(off_perm_to_perm)} object(s) from off-perm to perm bucket:"
        )
        for obj_key in off_perm_to_perm:
            logging.info(obj_key)
        logging.info(
            f"Copying {len(perm_to_off_perm)} object(s) from perm to off-perm bucket:"
        )
        for obj_key in perm_to_off_perm:
            logging.info(obj_key)
        logging.info(
            f"Deleting {len(delete_from_temp)} redundant object(s) from temp bucket:"
        )
        for obj_key in delete_from_temp:
            logging.info(obj_key)

        with TemporaryDirectory() as temp_dir:
            for obj_key in temp_to_perm:
                temp_bucket.download_file(obj_key, os.path.join(temp_dir, obj_key))
                # Verify checksum because we can't trust that the objects in the temp bucket has correct checksums
                # i.e. attackers can simply use a custom client to upload objects with arbitrary names
                with open(os.path.join(temp_dir, obj_key), "rb") as f:
                    checksum = sha256(f.read()).hexdigest()
                if checksum != obj_key:
                    errors.append(
                        ValueError(
                            f"Checksum mismatch for object {obj_key} in temp bucket! Not uploading to perm bucket."
                        )
                    )
                    continue

                perm_bucket.upload_file(os.path.join(temp_dir, obj_key), obj_key)
                temp_bucket.delete_objects(Delete={"Objects": [{"Key": obj_key}]})

            for obj_key in off_perm_to_perm:
                off_perm_bucket.download_file(obj_key, os.path.join(temp_dir, obj_key))
                perm_bucket.upload_file(os.path.join(temp_dir, obj_key), obj_key)
                off_perm_bucket.delete_objects(Delete={"Objects": [{"Key": obj_key}]})

            for obj_key in perm_to_off_perm:
                perm_bucket.download_file(obj_key, os.path.join(temp_dir, obj_key))
                off_perm_bucket.upload_file(os.path.join(temp_dir, obj_key), obj_key)
                perm_bucket.delete_objects(Delete={"Objects": [{"Key": obj_key}]})

            for obj_key in delete_from_temp:
                temp_bucket.delete_objects(Delete={"Objects": [{"Key": obj_key}]})

        if errors:
            logging.error("Encountered the following errors during execution:")
            for error in errors:
                logging.error(error)
            raise ValueError(f"Encountered {len(errors)} errors during agent execution. Please see above for details.")

        logging.info("Agent execution complete")
