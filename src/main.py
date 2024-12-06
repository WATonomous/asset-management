import json
import os
from watcloud_utils.typer import app

from .agent import Agent

WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", "/tmp/workspace")

@app.command()
def run_agent():
    agent = Agent(json.loads(os.environ["BUCKET_CONFIG"]), json.loads(os.environ["REPO_CONFIG"]), WORKSPACE_DIR)
    agent.run()

if __name__ == "__main__":
    app()