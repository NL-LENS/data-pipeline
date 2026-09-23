"""Read files, store metadata and save as Parquet."""

import logging
import os
from collections.abc import Generator
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from functools import partial
from pathlib import Path
from time import perf_counter
import duckdb
import polars as pl
import pyarrow.parquet as pq
import pyreadstat
from data_pipeline.metadata import get_file_stats
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest
from data_pipeline.utils import quote_identifier

DEFAULT_CHUNKSIZE = 500_000

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
        )
        sav_metadata.append(record)

    return sav_metadata


def iter_sav_chunks(
    source_file: Path | str,
    chunk_size: int = DEFAULT_CHUNKSIZE,
    num_processes: int | None = None,
    offset_workaround: bool = False,
) -> Generator[pl.DataFrame, None, None]:
    """Yield SAV data as Polars chunks using multiprocessing.

    With ``offset_workaround=True``, read explicit row offsets instead of
    ``read_file_in_chunks``. The built-in chunk reader receives a partially
    configured SAV reader so its internal metadata read also uses Polars.
    """
    if chunk_size < 1:
        msg = "chunk_size must be positive"
        raise ValueError(msg)
    if num_processes is None:
        num_processes = max(1, (os.cpu_count() or 1) - 1)
    if num_processes < 1:
        msg = "num_processes must be positive"
        raise ValueError(msg)

    if offset_workaround:
        total_rows = read_sav_meta(source_file).number_rows or 0
        offset = 0
        while offset < total_rows:
            chunk, _ = pyreadstat.read_file_multiprocessing(
                pyreadstat.read_sav,
                source_file,
                row_offset=offset,
                row_limit=min(chunk_size, total_rows - offset),
                num_processes=num_processes,
                output_format="polars",
            )
            if len(chunk) == 0:
                msg = f"SAV reader returned no rows at offset {offset}, expected {total_rows} rows"
                raise RuntimeError(msg)
            yield chunk
            offset += len(chunk)
        return

    reader = pyreadstat.read_file_in_chunks(
        partial(pyreadstat.read_sav, output_format="polars"),
        source_file,
        chunksize=chunk_size,
        multiprocess=True,
        num_processes=num_processes,
        output_format="polars",
    )
    for chunk, _ in reader:
        yield chunk


def stream_to_bronze(  # noqa: PLR0913, PLR0915
    source_file: Path | str,
    dest_file: Path | str,
    chunk_size: int,
    num_processes: int | None = None,
    benchmark: bool = False,
    offset_workaround: bool = False,
) -> None:
    """Stream a source SAV file to bronze Parquet.

    Parameters
    ----------
    source_file : Path or str
        Path to the source file.
    dest_file : Path or str
        Path to the output Parquet file.
    chunk_size : int
        Number of rows to read per chunk.
    num_processes : int or None
        Number of worker processes. Defaults to visible CPUs minus one.
    benchmark : bool
        Process at least 10% and write it to a separate benchmark file.
    offset_workaround : bool
        Read chunks using explicit row offsets.
    """
    if chunk_size < 1:
        msg = "chunk_size must be positive"
        raise ValueError(msg)
    if num_processes is None:
        num_processes = max(1, (os.cpu_count() or 1) - 1)
    if num_processes < 1:
        msg = "num_processes must be positive"
        raise ValueError(msg)

    meta = read_sav_meta(source_file)
    total_rows = meta.number_rows or 0
    if benchmark:
        dest_file = Path(dest_file)
        dest_file = dest_file.with_name(f"{dest_file.stem}.benchmark{dest_file.suffix}")

    print(f"CPUs visible: {os.cpu_count()}")  # noqa: T201
    print(f"Processes used: {num_processes}")  # noqa: T201
    print(f"Chunk size: {chunk_size}")  # noqa: T201
    print(f"Offset workaround: {offset_workaround}")  # noqa: T201
    if benchmark:
        print("Benchmarking processing time for 10% data.")  # noqa: T201

    sample_table, _ = pyreadstat.read_sav(source_file, output_format="polars", row_limit=1000)
    sample_table.columns = [col.upper() for col in sample_table.columns]
    parquet_schema = sample_table.to_arrow().schema
    reader = iter_sav_chunks(source_file, chunk_size, num_processes, offset_workaround)
    total_start = perf_counter()
    rows_written = 0
    chunk_number = 0
    try:
        with pq.ParquetWriter(dest_file, parquet_schema) as writer:
            while not benchmark or rows_written < total_rows * 0.10:
                read_start = perf_counter()
                try:
                    chunk = next(reader)
                except StopIteration:
                    break
                read_elapsed = perf_counter() - read_start
                write_start = perf_counter()
                chunk.columns = [col.upper() for col in chunk.columns]
                writer.write_table(chunk.to_arrow())
                write_elapsed = perf_counter() - write_start
                rows_written += len(chunk)
                chunk_number += 1
                progress = rows_written / total_rows if total_rows else 0
                print(  # noqa: T201
                    f"Chunk {chunk_number:3d} | {len(chunk):,} rows | "
                    f"read {read_elapsed:7.2f}s | write {write_elapsed:6.2f}s | "
                    f"{rows_written:,}/{total_rows:,} ({progress:.1%})"
                )
    finally:
        reader.close()

    total_elapsed = perf_counter() - total_start
    print(f"\nFinished {rows_written:,} rows")  # noqa: T201
    print(f"Total time: {total_elapsed:.2f}s")  # noqa: T201
    print(f"Total time: {total_elapsed / 60:.2f} min")  # noqa: T201
    if total_elapsed > 0 and benchmark:
        print(f"Average throughput: {rows_written / total_elapsed:,.0f} rows/s")  # noqa: T201


def write_column_metadata_to_db(db_file: Path | str, source_path: Path, source_filename: Path | str) -> None:
    """Parse table metadata and write to database."""
    sav_column_meta = parse_sav_meta(source_path, source_filename)

    table = SavMetaTable(db_file)
    table.create_from_record(sav_column_meta[0])
    table.insert_many(sav_column_meta)


def ingest_source(  # noqa: PLR0913
    source_manifest: SourceManifest,
    source_path: Path,
    source_filename: Path,
    dest_file: Path | str,
    chunk_size: int | None = None,
    benchmark: bool = False,
    offset_workaround: bool = False,
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
    benchmark:
        Write a benchmark sample without updating ingestion metadata.
    offset_workaround:
        Read chunks using explicit row offsets.

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
    stream_to_bronze(
        full_path_to_sav_file,
        dest_file,
        chunk_size,
        benchmark=benchmark,
        offset_workaround=offset_workaround,
    )
    if benchmark:
        return

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
    chunk_size: int | None = None,
    benchmark: bool = False,
    offset_workaround: bool = False,
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
    benchmark:
        Process a sample into a separate benchmark file.
    offset_workaround:
        Read chunks using explicit row offsets.

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
        ingest_source(
            source_manifest,
            Path(source_path),
            Path(source_filename),
            dest_filename,
            chunk_size,
            benchmark=benchmark,
            offset_workaround=offset_workaround,
        )


# ruff: enable[PLR0913, PLR0917]
