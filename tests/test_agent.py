from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from git import Repo
from moto import mock_aws
from watcloud_utils.logging import logger, set_up_logging

from src.agent import Agent

set_up_logging()


def set_up_buckets():
    s3 = boto3.resource("s3")

    bucket_config = {
        name: {
            "endpoint_url": None,  # this makes boto use the mock endpoint
            "access_key_id": "dummy-access-key-id",
            "secret_key": "dummy-secret-key",
            "bucket_name": f"asset-{name}-test",
        }
        for name in ["temp", "perm", "off-perm"]
    }

    for config in bucket_config.values():
        s3.create_bucket(Bucket=config["bucket_name"])

    return bucket_config


def set_up_repo(path):
    repo = Repo.init(path, initial_branch="main")
    (Path(path) / "file.txt").write_text("test content")
    repo.index.add(["file.txt"])
    repo.index.commit("initial commit")
    return repo


def commit_to_repo(repo, filename, contents, branch="main", from_branch="main"):
    # create branch if it doesn't exist
    if branch not in repo.branches:
        repo.create_head(branch, from_branch)
    
    # checkout branch
    repo.git.checkout(branch)

    # write file
    (Path(repo.working_tree_dir) / filename).write_text(contents)
    repo.index.add([filename])
    repo.index.commit(f"commit {filename}")


@mock_aws
def test_empty():
    """
    A test that runs the agent with a trivial repo and empty buckets. Just a sanity check.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        bucket_config = set_up_buckets()
        set_up_repo(repo_dir)

        repo_config = {"repos": [{"type": "local", "path": repo_dir}]}

        agent = Agent(bucket_config, repo_config, workspace_dir)

        agent.run()

        for config in bucket_config.values():
            assert (
                len(
                    list(
                        boto3.resource("s3").Bucket(config["bucket_name"]).objects.all()
                    )
                )
                == 0
            )


@mock_aws
def test_temp_to_perm():
    """
    This test simulates an upload to the temp bucket and committing the WATcloud URI to the repo.

    The agent should move the file from the temp bucket to the perm bucket.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        bucket_config = set_up_buckets()
        repo = set_up_repo(repo_dir)

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        test_content = b"some test content"
        test_content_sha256 = sha256(test_content).hexdigest()
        temp_bucket.put_object(Key=test_content_sha256, Body=test_content)

        commit_to_repo(repo, "file.txt", f"watcloud://v1/sha256:{test_content_sha256}")

        repo_config = {"repos": [{"type": "local", "path": repo_dir}]}

        agent = Agent(bucket_config, repo_config, workspace_dir)

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 1
        assert (
            perm_bucket.Object(test_content_sha256).get()["Body"].read() == test_content
        )

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 0


@mock_aws
def test_perm_to_off_perm():
    """
    This test simulates removing a WATcloud URI from the latest commit in the repo.

    The agent should move the file from the perm bucket to the off-perm bucket.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        bucket_config = set_up_buckets()
        repo = set_up_repo(repo_dir)

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        test_content = b"some test content"
        test_content_sha256 = sha256(test_content).hexdigest()
        perm_bucket.put_object(Key=test_content_sha256, Body=test_content)

        commit_to_repo(repo, "file.txt", f"watcloud://v1/sha256:{test_content_sha256}")
        commit_to_repo(repo, "file.txt", "watcloud://v1/sha256:dummy-sha256")

        repo_config = {"repos": [{"type": "local", "path": repo_dir}]}

        agent = Agent(bucket_config, repo_config, workspace_dir)

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 0

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 1
        assert (
            off_perm_bucket.Object(test_content_sha256).get()["Body"].read()
            == test_content
        )


@mock_aws
def test_off_perm_to_perm():
    """
    This test simulates adding back a WATcloud URI to the latest commit in the repo.

    The agent should move the file from the off-perm bucket to the perm bucket.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        bucket_config = set_up_buckets()
        repo = set_up_repo(repo_dir)

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        test_content = b"some test content"
        test_content_sha256 = sha256(test_content).hexdigest()
        off_perm_bucket.put_object(Key=test_content_sha256, Body=test_content)

        commit_to_repo(repo, "file.txt", "watcloud://v1/sha256:dummy-sha256")
        commit_to_repo(repo, "file.txt", f"watcloud://v1/sha256:{test_content_sha256}")

        repo_config = {"repos": [{"type": "local", "path": repo_dir}]}

        agent = Agent(bucket_config, repo_config, workspace_dir)

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 1
        assert (
            perm_bucket.Object(test_content_sha256).get()["Body"].read() == test_content
        )

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 0


@mock_aws
def test_agent_lifecycle():
    """
    This test simulates the full lifecycle of the agent with a single file.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        # Adding a file to the temp bucket and committing the WATcloud URI to the repo.
        # The agent should move the file from the temp bucket to the perm bucket.
        bucket_config = set_up_buckets()
        repo = set_up_repo(repo_dir)

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        test_content = b"some test content"
        test_content_sha256 = sha256(test_content).hexdigest()
        temp_bucket.put_object(Key=test_content_sha256, Body=test_content)

        commit_to_repo(repo, "file.txt", f"watcloud://v1/sha256:{test_content_sha256}")

        agent = Agent(
            bucket_config,
            {"repos": [{"type": "local", "path": repo_dir}]},
            workspace_dir,
        )

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 1
        assert (
            perm_bucket.Object(test_content_sha256).get()["Body"].read() == test_content
        )

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 0

        # Removing the WATcloud URI from the latest commit.
        # The agent should move the file from the perm bucket to the off-perm bucket.
        commit_to_repo(repo, "file.txt", "watcloud://v1/sha256:dummy-sha256")

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 0

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 1
        assert (
            off_perm_bucket.Object(test_content_sha256).get()["Body"].read()
            == test_content
        )

        # Adding back the WATcloud URI to the latest commit.
        # The agent should move the file from the off-perm bucket to the perm bucket.
        commit_to_repo(repo, "file.txt", f"watcloud://v1/sha256:{test_content_sha256}")

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 1
        assert (
            perm_bucket.Object(test_content_sha256).get()["Body"].read() == test_content
        )

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 0


@mock_aws
def test_files_on_different_branches():
    """
    This test simulates adding a file to a new branch.
    
    The agent should move the file from the temp bucket to the perm bucket.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        bucket_config = set_up_buckets()
        repo = set_up_repo(repo_dir)

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        test_content = b"some test content"
        test_content_sha256 = sha256(test_content).hexdigest()
        temp_bucket.put_object(Key=test_content_sha256, Body=test_content)

        commit_to_repo(repo, "file.txt", f"watcloud://v1/sha256:{test_content_sha256}", branch="new-branch")

        repo_config = {"repos": [{"type": "local", "path": repo_dir}]}

        agent = Agent(bucket_config, repo_config, workspace_dir)

        agent.run()

        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        assert len(list(temp_bucket.objects.all())) == 0

        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        assert len(list(perm_bucket.objects.all())) == 1
        assert (
            perm_bucket.Object(test_content_sha256).get()["Body"].read() == test_content
        )

        off_perm_bucket = boto3.resource("s3").Bucket(
            bucket_config["off-perm"]["bucket_name"]
        )
        assert len(list(off_perm_bucket.objects.all())) == 0

@mock_aws
def test_multiple_files_multiple_branches():
    """
    This test simulates the full lifecycle of the agent with multiple files on multiple branches.
    """
    with TemporaryDirectory() as workspace_dir, TemporaryDirectory() as repo_dir:
        bucket_config = set_up_buckets()
        repo = set_up_repo(repo_dir)
        repo_config = {"repos": [{"type": "local", "path": repo_dir}]}
        agent = Agent(bucket_config, repo_config, workspace_dir)
        temp_bucket = boto3.resource("s3").Bucket(bucket_config["temp"]["bucket_name"])
        perm_bucket = boto3.resource("s3").Bucket(bucket_config["perm"]["bucket_name"])
        off_perm_bucket = boto3.resource("s3").Bucket(bucket_config["off-perm"]["bucket_name"])

        # Upload file1 and use it in branch1
        test_content1 = b"some test content 1"
        test_content_sha256_1 = sha256(test_content1).hexdigest()
        temp_bucket.put_object(Key=test_content_sha256_1, Body=test_content1)

        commit_to_repo(repo, "file1_uri.txt", f"watcloud://v1/sha256:{test_content_sha256_1}", branch="branch1")

        # Upload file2 and use it in branch2
        test_content2 = b"some test content 2"
        test_content_sha256_2 = sha256(test_content2).hexdigest()
        temp_bucket.put_object(Key=test_content_sha256_2, Body=test_content2)

        commit_to_repo(repo, "file2_uri.txt", f"watcloud://v1/sha256:{test_content_sha256_2}", branch="branch2")

        # Upload file3 and use it in branch3
        test_content3 = b"some test content 3"
        test_content_sha256_3 = sha256(test_content3).hexdigest()
        temp_bucket.put_object(Key=test_content_sha256_3, Body=test_content3)

        commit_to_repo(repo, "file3_uri.txt", f"watcloud://v1/sha256:{test_content_sha256_3}", branch="branch3")

        # Run the agent
        agent.run()

        # Check that the files are in the perm bucket
        assert len(list(temp_bucket.objects.all())) == 0
        assert len(list(perm_bucket.objects.all())) == 3
        assert perm_bucket.Object(test_content_sha256_1).get()["Body"].read() == test_content1
        assert perm_bucket.Object(test_content_sha256_2).get()["Body"].read() == test_content2
        assert perm_bucket.Object(test_content_sha256_3).get()["Body"].read() == test_content3
        assert len(list(off_perm_bucket.objects.all())) == 0

        # Remove the WATcloud URI for file1
        commit_to_repo(repo, "file1_uri.txt", "watcloud://v1/sha256:dummy-sha256", branch="branch1")

        # Run the agent
        agent.run()

        # Check that file1 is in the off-perm bucket
        assert len(list(temp_bucket.objects.all())) == 0
        assert len(list(perm_bucket.objects.all())) == 2
        assert perm_bucket.Object(test_content_sha256_2).get()["Body"].read() == test_content2
        assert perm_bucket.Object(test_content_sha256_3).get()["Body"].read() == test_content3
        assert len(list(off_perm_bucket.objects.all())) == 1
        assert off_perm_bucket.Object(test_content_sha256_1).get()["Body"].read() == test_content1

        # Add the WATcloud URI for file1 to branch2
        commit_to_repo(repo, "file1_uri.txt", f"watcloud://v1/sha256:{test_content_sha256_1}", branch="branch2")

        # Run the agent
        agent.run()

        # Check that file1 is back in the perm bucket
        assert len(list(temp_bucket.objects.all())) == 0
        assert len(list(perm_bucket.objects.all())) == 3
        assert perm_bucket.Object(test_content_sha256_1).get()["Body"].read() == test_content1
        assert perm_bucket.Object(test_content_sha256_2).get()["Body"].read() == test_content2
        assert perm_bucket.Object(test_content_sha256_3).get()["Body"].read() == test_content3
        assert len(list(off_perm_bucket.objects.all())) == 0

        # Remove the WATcloud URI for file2
        commit_to_repo(repo, "file2_uri.txt", "watcloud://v1/sha256:dummy-sha256", branch="branch2")

        # Run the agent
        agent.run()

        # Check that file2 is in the off-perm bucket
        assert len(list(temp_bucket.objects.all())) == 0
        assert len(list(perm_bucket.objects.all())) == 2
        assert perm_bucket.Object(test_content_sha256_1).get()["Body"].read() == test_content1
        assert perm_bucket.Object(test_content_sha256_3).get()["Body"].read() == test_content3
        assert len(list(off_perm_bucket.objects.all())) == 1
        assert off_perm_bucket.Object(test_content_sha256_2).get()["Body"].read() == test_content2

        # Remove the WATcloud URI for file3
        commit_to_repo(repo, "file3_uri.txt", "watcloud://v1/sha256:dummy-sha256", branch="branch3")

        # Run the agent
        agent.run()

        # Check that file3 is in the off-perm bucket
        assert len(list(temp_bucket.objects.all())) == 0
        assert len(list(perm_bucket.objects.all())) == 1
        assert perm_bucket.Object(test_content_sha256_1).get()["Body"].read() == test_content1
        assert len(list(off_perm_bucket.objects.all())) == 2
        assert off_perm_bucket.Object(test_content_sha256_2).get()["Body"].read() == test_content2
        assert off_perm_bucket.Object(test_content_sha256_3).get()["Body"].read() == test_content3
