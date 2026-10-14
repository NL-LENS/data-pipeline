# Data build system for life event registry data

This is a tool to manage the data pipeline for life event registry data.

Rough steps:
1. Collect metadata about raw file from data provider
2. Read raw data and save as parquet
3. Minimally process raw data to a silver layer in event format

## Setup

```bash
uv sync

source .venv/bin/activate
pytest
```

### Architecture

The core is a database with metadata about files and `.sav` tables
used for registry research.
- All *actual data* (content of `.sav` tables)
is not part of the database, but stored in parquet files.
- The idea is that the database stores all information on the available and used
 datasets in a registry data project, and provides information relevant for
 data processing at various stages.

### Configuration files

`configs/silver_config.yaml` tells the silver pipeline how to turn each bronze dataset into events. It has one entry per dataset under `datasets:`, keyed by the
dataset name (e.g. `SPOLISBUS`):

| Key | Meaning |
|---|---|
| `CUMULATIVE` | `true` if each release contains the full history (so only the latest one is needed), `false` if each release is a separate snapshot |
| `PRIMARY_IDS` | Columns that identify the unit an event belongs to (e.g. `RINPERSOON`, `RINPERSOONS`) |
| `SECONDARY_IDS` | Other ID columns kept as event attributes (e.g. the related person in network tables) |
| `TIME_VARIABLES` | The event time: either a date column, or a fixed reference date (peilmoment) written as `"MM-DD"` and combined with the release year.  |



## Development

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



