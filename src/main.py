from .agent import Agent
import os

WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", "/tmp/workspace")

def main():
    agent = Agent(os.environ["BUCKET_CONFIG"], os.environ["REPO_CONFIG"], WORKSPACE_DIR)
    agent.run()

if __name__ == "__main__":
    main()