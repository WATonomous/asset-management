import os
import subprocess

import boto3

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
S3CFG_PERM_PATH = FILE_PATH + "/s3cfg_perm"
S3CFG_TEMP_PATH = FILE_PATH + "/s3cfg_temp"
S3_TEMP = "s3://asset-temp"
S3_PERM = "s3://asset-perm"
TEMP_ASSET_DIR = "temp"

bucket_map = {
    S3_TEMP: S3CFG_TEMP_PATH,
    S3_PERM: S3CFG_PERM_PATH
}

# Setup boto3 connection clients
host_base = "https://rgw.watonomous.ca"

access_temp = os.getenv("ACCESS_TEMP")
secret_temp = os.getenv("SECRET_TEMP")
client_temp = boto3.resource(
    's3',
    endpoint_url=host_base,
    aws_access_key_id=access_temp,
    aws_secret_access_key=secret_temp,
)
bucket_temp = None
for bucket in client_temp.buckets.all():
    bucket_temp = bucket

access_perm = os.getenv("ACCESS_PERM")
secret_perm = os.getenv("SECRET_PERM")
client_perm = boto3.resource(
    's3',
    endpoint_url=host_base,
    aws_access_key_id=access_perm,
    aws_secret_access_key=secret_perm,
)
bucket_perm = None
for bucket in client_perm.buckets.all():
    bucket_perm = bucket

# Helper to use s3cmd
def run_command(cmd):
    # Convert cmd string into args form
    cmd_args = []
    for word in cmd.split():
        cmd_args.append(word)

    try:
        # print(f'Command {cmd_args}:')
        result = subprocess.run(cmd_args, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                cwd="infra-config")
        return result.stdout

    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)
        print("Command Output:", e.stdout)
        print("Command Error:", e.stderr)

def get_assets_in_bucket(bucket):
    assets = []
    for obj in bucket.objects.all():
        assets.append(obj.key)
    
    return assets

def get_assets_in_repo():
    """
    Returns a list of URI hashes in repo.

    URI hashes in repo has the form "watcloud://v1//sha256:...?name=filename.extension",
    return just the "..."
    """
    raw_output = run_command(f'git grep watcloud://v1/sha256:')

    print("uris:")
    print(raw_output)

    # filename -> filepath relative to website/
    uris = []
    if not raw_output:
        print("No matching URI found")
        return None

    for output in raw_output.split("\n"):
        start = output.rfind("watcloud://v1/sha256:") + 21 # watcloud://v1/sha256 is 20 chars
        end = start + 64 # sha256 has 256 bits = 64 characters
        if len(output[start:end]) == 64:
            uris.append(output[start:end])
        

    return uris

def compare_s3_to_repo():
    """
    Returns 2 lists of filenames for what to remove from s3 and what to upload to s3
    """
    temp_s3_uris = get_assets_in_bucket(bucket_temp)

    perm_s3_uris = get_assets_in_bucket(bucket_perm)

    repo_uris = get_assets_in_repo()

    # Lists containing uris 
    to_perm = []
    to_temp = []

    # No changes just exit
    if repo_uris == None:
        return [], []

    # Files to add to perm storage
    for repo_uri in repo_uris:
        if (repo_uri not in perm_s3_uris) and (repo_uri in temp_s3_uris):
            to_perm.append(repo_uri)

    # Files to remove from perm storage
    for perm_uri in perm_s3_uris:
        if perm_uri not in repo_uris:
            to_temp.append(perm_uri)

    return to_perm, to_temp

# def upload_file(filepath, bucket_uri):
#     run_command(f's3cmd --config={bucket_map[bucket_uri]} put {filepath} {bucket_uri}')
#     print("Uploaded file")

def upload_file(filepath, bucket):
    bucket.upload_file(filepath, os.path.basename(filepath))
    print(f"Uploaded {filepath} to {bucket}")

def download_file(filename, folder_path, bucket):
    bucket.download_file(filename, folder_path)
    print(f"Downloaded {filename} to {folder_path}")

def delete_file(filename, bucket):
    objs = bucket_perm.objects.all()
    for obj in objs:
        print(obj)
    bucket.Object(filename).delete()
    print(f"Deleted {filename} from {bucket}")
    objs = bucket_perm.objects.all()
    for obj in objs:
        print(obj)

def transfer_file(filename, from_bucket, to_bucket):
    # Download file
    download_file(filename, TEMP_ASSET_DIR + "/" + filename, from_bucket)

    # Delete file in the old bucket
    # run_command(f's3cmd --config={bucket_map[from_bucket]} del {from_bucket}/{filename}')
    delete_file(filename, from_bucket)

    # Upload file to new bucket
    upload_file(f'{tmp_dir}/{filename}', to_bucket)

    # Delete local temp file
    # os.remove(f'{tmp_dir}/{filename}')

    print("File Transfer Done!")

def manage_assets():
    """
    Scans repo for asset URIs and checks temp and perm s3 buckets for them.
    Asset in repo & asset in temp & asset not in perm: move asset from temp -> perm
    Asset not repo & asset in perm: move asset perm -> temp
    """

    move_to_perm, move_to_temp = compare_s3_to_repo()

    print(f'Moving to perm: {move_to_perm}')
    print(f'Moving to temp: {move_to_temp}')

    for filename in move_to_perm:
        transfer_file(filename, bucket_temp, bucket_perm)

    for filename in move_to_temp:
        transfer_file(filename, bucket_perm, bucket_perm)

def clean_bucket():
    assets = get_assets_in_bucket(S3_PERM)
    for asset in assets:
        run_command(f's3cmd --config={bucket_map[S3_PERM]} del {S3_PERM}/{asset}')

    assets = get_assets_in_bucket(S3_TEMP)
    for asset in assets:
        run_command(f's3cmd --config={bucket_map[S3_TEMP]} del {S3_TEMP}/{asset}')

if __name__ == "__main__":
    # Make temp dir for assets if needed.
    os.makedirs(TEMP_ASSET_DIR, exist_ok=True)

    # Run script
    manage_assets()