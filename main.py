import os
import hashlib
import subprocess

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
S3CFG_PATH = FILE_PATH + "/s3cfg"
S3_TEMP = "s3://asset-temp"
S3_PERM = "s3://asset-perm"

# Helper to use s3cmd
def run_command(cmd):
    # Convert cmd string into args form
    cmd_args = []
    for word in cmd.split():
        cmd_args.append(word)

    try:
        # print(f'Command {cmd_args}:')
        result = subprocess.run(cmd_args, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                cwd="../infra-config")
        return result.stdout

    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)
        print("Command Output:", e.stdout)
        print("Command Error:", e.stderr)

def encode_sha256(plaintext):
    encoded = plaintext.encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()

def get_sha256_dict(plaintexts):
    """
    Returns dict(hash, plaintext)
    """
    output = {}
    for text in plaintexts:
        output[encode_sha256(text)] = text
    return output

def get_assets_in_bucket(bucket_uri):
    """
    Returns a list of URI hashes in the bucket
    """
    raw_output = run_command(f's3cmd --config={S3CFG_PATH} ls {bucket_uri}')
    files = [word for word in raw_output.split() if "s3://" in word]

    for i, filename in enumerate(files):
        back_slash = filename.rfind("/") + 1
        files[i] = filename[back_slash:]
        
    return files

def get_assets_in_repo():
    """
    Returns a list of URI hashes in repo.

    URI hashes in repo has the form "watcloud://v1//sha256:...?name=filename.extension",
    return just the "..."
    """
    raw_output = run_command(f'git grep watcloud://v1/sha256:')

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
    temp_s3_uris = get_assets_in_bucket("s3://asset-temp")

    perm_s3_uris = get_assets_in_bucket("s3://asset-perm")

    repo_uris = get_assets_in_repo()

    # Lists containing uris 
    to_perm = []
    to_temp = []

    # Files to add to perm storage
    for repo_uri in repo_uris:
        if (repo_uri not in perm_s3_uris) and (repo_uri in temp_s3_uris):
            to_perm.append(repo_uri)

    # Files to remove from perm storage
    for perm_uri in perm_s3_uris:
        if perm_uri not in repo_uris:
            to_temp.append(perm_uri)

    return to_perm, to_temp

def upload_file(filepath, bucket_uri):
    run_command(f's3cmd --config={S3CFG_PATH} put {filepath} {bucket_uri}')
    print("Uploaded file")

def download_file(filename, folder_path, bucket_uri):
    run_command(f's3cmd --config={S3CFG_PATH} get {bucket_uri}/{filename} {folder_path}/{filename}')
    print(f"Downloaded file to {folder_path}/{filename}")

def transfer_file(filename, from_bucket, to_bucket):
    tmp_dir = "/scripts/tmp"
    download_file(filename, tmp_dir, from_bucket)
    run_command(f's3cmd --config={S3CFG_PATH} del {from_bucket}/{filename}')
    upload_file(f'{tmp_dir}/{filename}', to_bucket)

    # Delete file
    os.remove(f'{tmp_dir}/{filename}')

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
        transfer_file(filename, S3_TEMP, S3_PERM)

    for filename in move_to_temp:
        transfer_file(filename, S3_PERM, S3_TEMP)

def clean_bucket():
    assets = get_assets_in_bucket(S3_PERM)
    for asset in assets:
        run_command(f's3cmd --config={S3CFG_PATH} del {S3_PERM}/{asset}')

    assets = get_assets_in_bucket(S3_TEMP)
    for asset in assets:
        run_command(f's3cmd --config={S3CFG_PATH} del {S3_TEMP}/{asset}')

if __name__ == "__main__":
    # manage_assets()
    # clean_bucket()

    # "server-room-light-min.jpg" -> watcloud://v1/sha256:96fc3a9fe38828d5db6146e9f8ddff0556f108fd1097f4c8a8c26721a01af557
    # "text2" -> watcloud://v1/sha256:d848ca35a6281600b5da598c7cb4d5df561e0ee63ee7cec0e98e6049996f3ff?name=text2.txt
    # print(get_sha256_dict(["server-room-dark-min", "server-room-light-min", "text2"]))

    print(get_assets_in_repo())

    # print("script ran")