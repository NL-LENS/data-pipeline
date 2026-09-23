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

## Command line

Install the project with `uv sync`, then initialize the source manifest:

```bash
uv run lens init --root /path/to/raw --db_file metadata.duckdb
uv run lens build bronze --db_file metadata.duckdb --source_regex EXAMPLE \
  --ref_period 2020 2021 --dest_dir data/bronze --offset_workaround
```

The build selects readable files with reference periods in the inclusive year
range. `--source_regex` is a SQL `LIKE` pattern matched within filenames (not a
regular expression). For each source path and reference period, only the highest
version is processed. Sources without a reference period are skipped.

SAV files are read with multiprocessing in chunks of 500,000 rows, using the
visible CPU count minus one (at least one worker). Use `--chunk_size` to change
the number of rows per chunk.

- `--offset_workaround` uses explicit row offsets instead of pyreadstat's chunk
  helper. Both paths use Polars, including metadata reads.
- `--benchmark` times whole chunks until at least 10% of each source is written.
  A small source can therefore be processed in full. Samples are written as
  `*.benchmark.parquet`, leaving normal output and ingestion metadata unchanged.
- Both flags default to false and take no value: use `--benchmark` and
  `--offset_workaround`, not `--benchmark true` or `--offset_workaround true`.
- Progress includes per-chunk read/write times and total elapsed time. Benchmark
  output also reports average throughput. Timing excludes the initial metadata
  and schema reads.
- Add `-v` for informational logging or `-vv` for debug logging.
