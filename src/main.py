import json
import os
from watcloud_utils.typer import app
from watcloud_utils.logging import set_up_logging

from .agent import Agent

WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", "/tmp/workspace")

set_up_logging()

@app.command()
def run_agent():
    agent = Agent(json.loads(os.environ["BUCKET_CONFIG"]), json.loads(os.environ["REPO_CONFIG"]), WORKSPACE_DIR)
    agent.run()

if __name__ == "__main__":
    app()