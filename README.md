# data-pipeline
Code for processing data

## Setup

```bash
uv sync
source .venv/bin/activate
pytest
```

## Development

Running CI locally with `act`

Assuming `act` is available and on the PATH:

```bash
act -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -W .github/workflows/$WORKFLOW_FILE.yml > .ci_log.txt
cat .ci_log.txt | grep Job # see status of each job run
rm -rf .ci_log.txt
```
