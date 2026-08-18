"""Read files, store metadata and save as Parquet."""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from pathlib import Path
import pyarrow.parquet as pq
import pyreadstat
from data_pipeline.metadata import get_file_stats
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest

DEFAULT_CHUNKSIZE = 100_000


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

    offset = 0

    with pq.ParquetWriter(dest_file, parquet_schema) as writer:
        while offset < total_rows:
            chunk, _ = pyreadstat.read_sav(
                source_file,
                row_offset=offset,
                row_limit=chunk_size,
                output_format="polars",
            )
            chunk.columns = [col.upper() for col in chunk.columns]
            table = chunk.to_arrow()
            writer.write_table(table)
            offset += chunk_size


def write_column_metadata_to_db(db_file: Path | str, source_path: Path, source_filename: Path | str) -> None:
    """Parse table metadata and write to database."""
    sav_column_meta = parse_sav_meta(source_path, source_filename)

    table = SavMetaTable(db_file)
    table.create_from_record(sav_column_meta[0])
    table.insert_many(sav_column_meta)


def ingest_source(db_file: Path | str, source_path: Path, source_filename: Path, dest_file: Path | str) -> None:
    """Ingest a source file from raw to bronze.

    The function does 3 things:
        - Process .sav file into bronze parquet.
        - Update the file metadata in the database.
        - Adds or updates the schema metadata in the database.

    Arguments
    ---------
    db_file:
        Path to the database with metadata.
    source_path:
        Path to the folder of `source_filename`.
    source_filename:
        Name of the .sav file to read.
    dest_file:
        The full path to the .parquet file to write.

    Raises
    ------
    DuckDB errors (TBD) if source_manifest table does not exist or
    when the specified .sav file is not in the database.
    """
    source_manifest = SourceManifest(db_file=db_file)

    lookup = {"source_path": source_path, "source_filename": source_filename}
    file_metadata = FileMetaRecord.from_table(lookup=lookup, table=source_manifest)

    full_path_to_sav_file = source_path / source_filename
    stream_to_bronze(full_path_to_sav_file, dest_file, DEFAULT_CHUNKSIZE)

    # Update file metadata
    last_modified, file_size_bytes = get_file_stats(full_path_to_sav_file)

    file_metadata.ingested_at = datetime.now(UTC)
    file_metadata.bronze_path = Path(dest_file).absolute()
    file_metadata.last_modified = last_modified
    file_metadata.file_size_bytes = file_size_bytes

    source_manifest.update(file_metadata)

    # Create table metadata
    # TODO: this creates duplicates because pre-existing data with same FK are not removed,
    # and no primary key constraint is in place.
    write_column_metadata_to_db(db_file, source_path, source_filename)
