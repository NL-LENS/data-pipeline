"""Read files, store metadata and save as Parquet."""

import logging
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
import pyreadstat
from tqdm import tqdm
from data_pipeline.metadata import get_file_stats
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest
from data_pipeline.utils import quote_identifier

DEFAULT_CHUNKSIZE = 1_000_000

logger = logging.getLogger(__name__)


def read_sav_meta(file: Path | str) -> pyreadstat.metadata_container:
    """Read metadata from .sav file."""
    _, meta = pyreadstat.read_sav(
        file,
        metadataonly=True,
        output_format="polars",
    )
    return meta


def parse_sav_meta(source_path: Path, source_filename: Path | str) -> Sequence[SavColumnMeta]:
    """Extract table metadata from a sav file.

    Arguments
    ---------
    source_file: file whose data to parse.

    Returns
    -------
    A list of SaveColumnMeta instances, each referring to
    one column in the `.sav` file, to be written to a database.
    """
    sav_metadata: list[SavColumnMeta] = []

    meta = read_sav_meta(Path(source_path) / source_filename)
    schema = meta.readstat_variable_types

    for var_name, readstat_type in schema.items():
        record = SavColumnMeta(
            source_path=Path(source_path),
            source_filename=Path(source_filename),
            variable=var_name.upper(),
            original_variable=var_name,
            readstat_type=readstat_type,
            description=meta.column_names_to_labels.get(var_name),
            value_labels=meta.variable_value_labels.get(var_name),
            variable_measure=meta.variable_measure.get(var_name)
        )
        sav_metadata.append(record)

    return sav_metadata


def stream_to_bronze(
    source_file: Path | str,
    dest_file: Path | str,
    chunk_size: int,
) -> None:
    """Stream a source .sav file to bronze Parquet.

    Parameters
    ----------
    source_file : Path or str
        Path to the source file.
    dest_file : Path or str
        Path to the output Parquet file.
    chunk_size : int
        Number of rows to read per chunk.
    """
    # NOTE: uses a work-around with a while loop + offsets + row_limit
    # instead of pyreadstat.read_file_in_chunks.
    # Reason: the latter passes pyreadstat.read_sav with output_format=`pandas`
    # for reading metadata, but leading to an implicit pandas runtime
    # dependency.
    meta = read_sav_meta(source_file)
    total_rows = meta.number_rows or 0

    # Read sample table for the schema; convert to upper
    sample_table, _ = pyreadstat.read_sav(source_file, output_format="polars", row_limit=1000)
    sample_table.columns = [col.upper() for col in sample_table.columns]
    parquet_schema = sample_table.to_arrow().schema

    with pq.ParquetWriter(dest_file, parquet_schema) as writer:
        for offset in tqdm(range(0, total_rows, chunk_size), unit="chunk", desc=Path(source_file).name):
            chunk, _ = pyreadstat.read_sav(
                source_file,
                row_offset=offset,
                row_limit=chunk_size,
                output_format="polars",
            )
            chunk.columns = [col.upper() for col in chunk.columns]
            table = chunk.to_arrow()
            writer.write_table(table)


def write_column_metadata_to_db(db_file: Path | str, source_path: Path, source_filename: Path | str) -> None:
    """Parse table metadata and write to database."""
    sav_column_meta = parse_sav_meta(source_path, source_filename)

    table = SavMetaTable(db_file)
    table.create_from_record(sav_column_meta[0])
    table.insert_many(sav_column_meta)


def ingest_source(
    source_manifest: SourceManifest,
    source_path: Path,
    source_filename: Path,
    dest_file: Path | str,
    chunk_size: int | None = None,
) -> None:
    """Ingest a source file from raw to bronze.

    The function does 3 things:
        - Process .sav file into bronze parquet.
        - Update the file metadata in the database.
        - Adds or updates the schema metadata in the database.

    Arguments
    ---------
    source_manifest:
        SourceManifest object to be used.
    source_path:
        Path to the folder of `source_filename`.
    source_filename:
        Name of the .sav file to read.
    dest_file:
        The full path to the .parquet file to write.
    chunk_size:
        Number of rows to process in one chunk.

    Raises
    ------
    DuckDB errors (TBD) if source_manifest table does not exist or
    when the specified .sav file is not in the database.
    """
    logger.info("Ingesting %s/%s", source_path, source_filename)

    lookup = {"source_path": source_path, "source_filename": source_filename}
    file_metadata = FileMetaRecord.from_table(lookup=lookup, table=source_manifest)

    full_path_to_sav_file = source_path / source_filename
    chunk_size = chunk_size or DEFAULT_CHUNKSIZE
    stream_to_bronze(full_path_to_sav_file, dest_file, chunk_size)

    # Update file metadata
    last_modified, file_size_mb = get_file_stats(full_path_to_sav_file)

    file_metadata.ingested_at = datetime.now(UTC)
    file_metadata.bronze_path = Path(dest_file).absolute()
    file_metadata.last_modified = last_modified
    file_metadata.file_size = file_size_mb

    source_manifest.update(file_metadata)

    # Create table metadata
    # TODO: this creates duplicates because pre-existing data with same FK are not removed,
    # and no primary key constraint is in place.
    write_column_metadata_to_db(source_manifest.db_file, source_path, source_filename)
    logger.info("Done.")


# ruff: disable[PLR0913, PLR0917]
def build(
    db_file: Path,
    source_regex: str,
    start_year: int,
    end_year: int,
    dest_dir: Path,
    chunk_size: int | None,
) -> None:
    """Build a set of bronze datasets.

    Arguments
    ---------
    db_file:
        Path to the database with metadata.
    source_regex:
        String to fuzzy-search the paths and file names for.
    start_year:
        First year for the reference period.
    end_year:
        Last year of the reference period.
    dest_dir:
        Destination directory to save the processed files to. The resulting
        filename is replicated from the filename of the source, with `.sav`
        replaced by `.parquet`. If it does not exist, it is created, including
        all parents.
    chunk_size:
        Number of rows to process per chunk.

    Notes
    -----
    For the reference period, start_year and end_year are both included.
    Within the datasets matched by source_regex,
    only the highest version within source_path and ref_period is used.
    Files without read permission are ignored.

    This function is currently experimental. For instance, datasets
    with missing reference period are ignored.
    """
    source_manifest = SourceManifest(db_file=db_file)

    with duckdb.connect(db_file) as con:
        # TODO: not sure yet what's the best way to query multiple
        # records in the tables. Plain sql for now.
        con.sql("SET TIMEZONE='UTC'")
        regex_param = f"%{source_regex}%"
        start_year_param = f"{start_year}-01-01"
        end_year_param = f"{end_year}-12-31"
        # ruff: disable[S608]
        sql = f"""
           SELECT source_path, source_filename
           FROM {quote_identifier(source_manifest.table_name)}
           WHERE
              source_filename like ?
              AND ref_period BETWEEN ? AND ?
              AND read_access
           QUALIFY
              row_number() OVER (
                  PARTITION BY source_path, ref_period ORDER BY version DESC
              ) = 1
        """
        # ruff: enable[S608]
        # NOTE: the `ORDER BY version DESC` could choose a random row
        # if version is NULL; however, it's not clear this can happen
        # within the primary key constraint of the table that guarantees that
        # file name are unique.
        params = (regex_param, start_year_param, end_year_param)
        datasets_to_process = con.execute(sql, params).fetchall()

    if len(datasets_to_process) == 0:
        return

    dest_dir.mkdir(exist_ok=True, parents=True)
    for source_path, source_filename in datasets_to_process:
        dest_filename = dest_dir / Path(source_filename).with_suffix(".parquet").name
        ingest_source(source_manifest, Path(source_path), Path(source_filename), dest_filename, chunk_size)


# ruff: enable[PLR0913, PLR0917]
