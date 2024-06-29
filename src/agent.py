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
    off_perm_bucket = get_bucket("off-perm")

    temp_objects = set(obj.key for obj in temp_bucket.objects.all())
    perm_objects = set(obj.key for obj in perm_bucket.objects.all())
    off_perm_objects = set(obj.key for obj in off_perm_bucket.objects.all())
    all_objects = temp_objects | perm_objects | off_perm_objects

    logging.info(f"Found {len(temp_objects)} objects in temp bucket")
    logging.info(f"Found {len(perm_objects)} objects in perm bucket")
    logging.info(f"Found {len(off_perm_objects)} objects in off-perm bucket")

    import pdb; pdb.set_trace()

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
    logging.info(f"Copying {len(off_perm_to_perm)} object(s) from off-perm to perm bucket:")
    for obj_key in off_perm_to_perm:
        logging.info(obj_key)
    logging.info(f"Copying {len(perm_to_off_perm)} object(s) from perm to off-perm bucket:")
    for obj_key in perm_to_off_perm:
        logging.info(obj_key)
    logging.info(f"Deleting {len(delete_from_temp)} redundant object(s) from temp bucket:")
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
        raise ValueError("Encountered errors during agent execution.")

    logging.info("Agent execution complete")


if __name__ == "__main__":
    app()
