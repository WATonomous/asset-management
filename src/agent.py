import logging
import os
from hashlib import sha256
from tempfile import TemporaryDirectory

from utils import app, clone_repos, flatten, get_watcloud_uris, get_bucket


@app.command()
def run_agent():
    logging.info("Starting agent")

    logging.info("Cloning repos")
    repos = list(clone_repos())

    logging.info("Extracting WATcloud URIs")
    watcloud_uris = list(
        # sorting to ensure consistent order for testing
        sorted(flatten([get_watcloud_uris(repo.working_dir) for repo in repos]))
    )

    logging.info(f"Found {len(watcloud_uris)} WATcloud URIs:")
    for uri in watcloud_uris:
        logging.info(uri)

    desired_perm_objects = set(uri.sha256 for uri in watcloud_uris)

    temp_bucket = get_bucket("temp")
    perm_bucket = get_bucket("perm")

    temp_objects = set(obj.key for obj in temp_bucket.objects.all())
    perm_objects = set(obj.key for obj in perm_bucket.objects.all())

    logging.info(f"Found {len(temp_objects)} objects in temp bucket")
    logging.info(f"Found {len(perm_objects)} objects in perm bucket")

    errors = []

    if not desired_perm_objects.issubset(temp_objects | perm_objects):
        errors.append(
            ValueError(
                f"Cannot find the following objects in any bucket: {desired_perm_objects - temp_objects - perm_objects}"
            )
        )

    # Objects that need to be copied to perm bucket
    to_perm = desired_perm_objects - perm_objects
    # Objects that need to be copied from temp bucket to perm bucket
    temp_to_perm = to_perm & temp_objects
    # Objects that need to be deleted from perm bucket
    perm_to_temp = perm_objects - desired_perm_objects
    # Objects that need to be deleted from the temp bucket (already exists in another bucket)
    delete_from_temp = desired_perm_objects & temp_objects - temp_to_perm

    logging.info(
        f"{len(desired_perm_objects&perm_objects)}/{len(desired_perm_objects)} objects are already in the perm bucket"
    )
    logging.info(f"Copying {len(temp_to_perm)} object(s) from temp to perm bucket:")
    for obj_key in temp_to_perm:
        logging.info(obj_key)
    logging.info(f"Copying {len(perm_to_temp)} object(s) from perm to temp bucket:")
    for obj_key in perm_to_temp:
        logging.info(obj_key)
    logging.info(f"Deleting {len(delete_from_temp)} redundant object(s) from temp bucket:")
    for obj_key in delete_from_temp:
        logging.info(obj_key)

    with TemporaryDirectory() as temp_dir:
        for obj_key in temp_to_perm:
            temp_bucket.download_file(obj_key, os.path.join(temp_dir, obj_key))
            # Verify checksum
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

        for obj_key in perm_to_temp:
            perm_bucket.download_file(obj_key, os.path.join(temp_dir, obj_key))
            temp_bucket.upload_file(os.path.join(temp_dir, obj_key), obj_key)
            perm_bucket.delete_objects(Delete={"Objects": [{"Key": obj_key}]})

        for obj_key in delete_from_temp:
            temp_bucket.delete_objects(Delete={"Objects": [{"Key": obj_key}]})

    if errors:
        logging.error("Encountered the following errors during execution:")
        for error in errors:
            logging.error(error)
        raise ValueError("Encountered errors during agent execution.")

    logging.info("Agent execution complete")


if __name__ == "__main__":
    app()
