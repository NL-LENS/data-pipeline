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

### Architecture

The core is a database with metadata about files and `.sav` tables
used for registry research.
- All *actual data* (content of `.sav` tables)
is not part of the database, but stored in parquet files.
- The idea is that the database stores all information on the available and used
 datasets in a registry data project, and provides information relevant for
 data processing at various stages.

#### The metadata database

The database holds two tables. They differ in what a single row represents.

##### `source_manifest` -- one row per source file

Written by `lens init`, which walks `--root` and records every `.sav` file it
finds. Later filled in further by `lens build bronze` as files get ingested.

| Column | Meaning |
| --- | --- |
| `source_path` | Directory holding the file. Part of the primary key. |
| `source_filename` | File name including suffix. Part of the primary key. |
| `read_access` | Whether the account running `init` could actually open the file. |
| `last_modified` | File modification time, UTC. |
| `file_size` | Size in MB. |
| `ref_period` | Reference period parsed from the file name, e.g. `INPA2017V3.sav` -> 2017-01-01. `NULL` when the name does not match the pattern. |
| `version` | Version number parsed from the file name, e.g. `INPA2017V3.sav` -> 3. |
| `bronze_path` | Path to the parquet file. `NULL` until ingested. |
| `schema_hash` | Reserved, currently always `NULL`. |
| `ingested_at` | When the file was converted to parquet. `NULL` until ingested. |

The `NULL`-until-ingested columns are how you tell which files are merely
*known* from those that have been *processed*.

##### `sav_meta` -- one row per column in a source file

Written by `lens build bronze` while ingesting a file. This is where the SPSS
column semantics live, since parquet cannot carry them.

| Column | Meaning |
| --- | --- |
| `source_path`, `source_filename` | Which file this column came from; foreign key into `source_manifest`. |
| `variable` | Upper-cased name, as written in the parquet file. |
| `original_variable` | Name as it appears in the `.sav` file. |
| `readstat_type` | Column type reported by pyreadstat, e.g. `double`, `string`. |
| `variable_measure` | SPSS measurement level: `nominal`, `ordinal`, `scale` or `unknown`. |
| `description` | The SPSS variable label. |
| `value_labels` | JSON map of value -> meaning. For categoricals, the category names; for numerics, typically the codes marking missingness. |

Actual data is never stored in the database -- only in the parquet files that
`bronze_path` points to.


## Development setup

```bash
uv sync --extra dev

source .venv/bin/activate
uv pip install -e ./
pytest
```


#### Running CI locally with `act`

Assuming `act` is available and on the PATH:

```bash
act -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -W .github/workflows/$WORKFLOW_FILE.yml > .ci_log.txt
cat .ci_log.txt | grep Job # see status of each job run
rm -rf .ci_log.txt
```
