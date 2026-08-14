# Data build system for life event registry data

This is a tool to manage the data pipeline for life event registry data.

Rough steps:
1. Collect metadata about raw file from data provider
2. Read raw data and save as parquet
3. Minimally process raw data to a silver layer in event format

## Usage -- experimental

```bash
python -m pip install 'data-pipeline @ git+https://github.com/NL-LENS/data-pipeline@cli'

lens init --db_file lens.duckdb --root G:/
lens build bronze \
  --db_file lens.duckdb \
  --source_regex INPATAB \
  --ref_period 2015 2020 \
  --dest_dir data_processed/bronze/
```

## Development setup

```bash
uv sync --extra dev

source .venv/bin/activate
pytest
```


#### Running CI locally with `act`

Assuming `act` is available and on the PATH:

```bash
act -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -W .github/workflows/$WORKFLOW_FILE.yml > .ci_log.txt
cat .ci_log.txt | grep Job # see status of each job run
rm -rf .ci_log.txt
```
