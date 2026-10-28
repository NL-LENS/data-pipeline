# Data build system for life event registry data

This is a tool to manage the data pipeline for life event registry data.

Rough steps:
1. Collect metadata about raw file from data provider
2. Read raw data and save as parquet
3. Minimally process raw data to a silver layer in event format

## Usage -- experimental

### 1. Initialize metadata db and create bronze data

```bash
python -m pip install 'data-pipeline @ git+https://github.com/NL-LENS/data-pipeline'

lens init --db_file lens.duckdb --root G:/
lens build bronze \
  --db_file lens.duckdb \
  --source_regex INPATAB \
  --ref_period 2015 2020 \
  --dest_dir data_processed/bronze/inpa


lens build bronze \
  --db_file lens.duckdb \
  --source_regex SPOLIS \
  --ref_period 2015 2020 \
  --dest_dir data_processed/bronze/spolis

```

### 2. Create silver data

Using a Python script as follows (note that metadata for silver are *not*
updated here; also `tqdm` needs to be added to the virtual environment)

```python
from data_pipeline.silver import convert_to_silver, SilverConfig
from data_pipeline.schemas import SourceManifest
from pathlib import Path
import duckdb
from tqdm import tqdm

import logging

logging.basicConfig(level=logging.INFO)


def main():

    db_file = "lens.duckdb"
    source_manifest = SourceManifest(db_file)

    event_dict = {
        "INPA": (
            "INPA",
            SilverConfig(
                rinpersoon_col="RINPERSOON",
                rinpersoons_col="RINPERSOONS",
                time_cols=[],
                id_cols=["RINPERSOON", "RINPERSOONHKW"],
                event_time_col=None,
                apply_event_time="01-01",
            ),
        ),
        "JOB_START": (
            "SPOLIS",
            SilverConfig(
                rinpersoon_col="RINPERSOON",
                rinpersoons_col="RINPERSOONS",
                time_cols=[
                    "SDATUMAANVANGIKO",
                    "SDATUMEINDEIKO",
                    "SDATUMAANVANGIKVORG",
                    "SDATUMAANVANGIKV",
                    "SDATUMEINDEIKV",
                ],
                id_cols=["RINPERSOON"],
                event_time_col="SDATUMAANVANGIKO",
                event_time_col_fmt="%Y%m%d",
            ),
        ),
        "JOB_END": (
            "SPOLIS",
            SilverConfig(
                rinpersoon_col="RINPERSOON",
                rinpersoons_col="RINPERSOONS",
                time_cols=[
                    "SDATUMAANVANGIKO",
                    "SDATUMEINDEIKO",
                    "SDATUMAANVANGIKVORG",
                    "SDATUMAANVANGIKV",
                    "SDATUMEINDEIKV",
                ],
                id_cols=["RINPERSOON"],
                event_time_col="SDATUMEINDEIKO",
                event_time_col_fmt="%Y%m%d",
            ),
        ),
    }

    with duckdb.connect(db_file, read_only=True) as con:
        con.sql("SET TIMEZONE='UTC'")
        for event, (design, config) in event_dict.items():
            sql = f"""
            SELECT source_path, source_filename, ref_period
            FROM '{source_manifest.table_name}'
            WHERE
                ingested_at IS NOT NULL
                AND source_path LIKE ?
            """
            regex_param = f"%{design}%"
            datasets_to_process = con.execute(sql, (regex_param,)).fetchall()
            for source_path, source_filename, ref_period in tqdm(datasets_to_process, desc=f"Creating {event}"):
                year = ref_period.year
                dest_path = Path(f"data_processed/silver/EVENT={event}") / f"{year}.parquet"
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                silver_meta = convert_to_silver(
                    source_manifest=source_manifest,
                    source_path=Path(source_path),
                    source_filename=Path(source_filename),
                    dest_path=dest_path,
                    config=config,
                )


if __name__ == "__main__":
    main()
```



### 3. Query the data

In a `DuckDB` terminal:

```sql
CREATE TABLE INPA as SELECT * FROM read_parquet('data_processed/silver/EVENT=INPA/');
CREATE TABLE JOB_START as SELECT * FROM read_parquet('data_processed/silver/EVENT=JOB_START/');
CREATE TABLE JOB_END as SELECT * FROM read_parquet('data_processed/silver/EVENT=JOB_END/');
SELECT MIN(DATE) from INPA;
SELECT MAX(DATE) from INPA;
SELECT MIN(DATE) from JOB_START;
SELECT MAX(DATE) from JOB_END;
```

## Architecture

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
