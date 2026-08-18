"""Defines all record and tables and how they relate to each other."""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path
from data_pipeline.duckdb_base import DuckDBBigInt
from data_pipeline.duckdb_base import DuckDBRecord
from data_pipeline.duckdb_base import DuckDBTable


@dataclass
class SourceManifest(DuckDBTable):
    """Table for the source manifest."""

    table_name = "source_manifest"


@dataclass
class SavMetaTable(DuckDBTable):
    """Table for the metadata of columns `.sav` tables."""

    table_name = "sav_meta"


@dataclass
class FileMetaRecord(DuckDBRecord):
    """Define the schema for file metadata records.

    Arguments
    ---------
    source_path:
        full path to the location of the file.
    source_filename:
        file name, including suffix.
    read_access:
        Indicates whether the file is read-accessible.
    last_modified:
        Date and time of last modification, in UTC format.
    file_size_bytes:
        Size of the file.
    ref_period:
        If available, the date and time of the reference period. See :func:`~extract_ref_period`.
    version
        If available, the integer version number. See :func:`~extract_version`.
    bronze_path:
        If ingested, the path to the bronze parquet file.
    schema_hash:
        Currently unused. In the future, might be the integer hash
        of the pyreadstat schema. Requires HUGEINT (INT128).
    ingested_at:
        If ingested, the date and time of ingestion.

    Example
    -------
    File on path `'G:/INPATAB/INPA2017V3.sav'` has
        - primary key on ('G:/INPATAB/', 'INPA2017V3.sav')
        - ref_period = datetime(2017, 1, 1)
        - version = 3
    """

    source_path: Path = field(metadata={"primary_key": True})
    source_filename: Path = field(metadata={"primary_key": True})
    read_access: bool
    last_modified: datetime
    file_size_bytes: int
    ref_period: datetime | None
    version: int | None
    bronze_path: Path | None = None
    schema_hash: DuckDBBigInt | None = None
    ingested_at: datetime | None = None


@dataclass
class SavColumnMeta(DuckDBRecord):
    """Container for metadata of columns in `.sav` files.

    Each instance of this class refers to one column in a `.sav` file, which
    is identified through the foreign key.
    When written to a table in the database, each *row* in the metadata table
    contains the data for one *column* in the table of the underlying `.sav` file.

    Arguments
    ---------
    source_path:
        full path to the location of the file.
    source_filename:
        file name, including suffix.
    variable:
        the upper-cased variable name, as it is stored in the parquet file.
    original_variable:
        the original variable name, as it is stored in the .sav file.
    readstat_type:
        the column type in the .sav file.
    description:
        the column description in the .sav file.
    value_labels:
        a dictionary where dict keys are numeric values in the columns, and
        dict values are the meaning of this value in the data.
        For string variables (=categorical variables), this is the meaning
        of the categories.
        For numeric variables, this contains the values indicating missingness.
    """

    source_path: Path = field(metadata={"foreign_key": {"table": "source_manifest", "column": "source_path"}})
    source_filename: Path = field(metadata={"foreign_key": {"table": "source_manifest", "column": "source_filename"}})
    variable: str
    original_variable: str
    readstat_type: str
    description: str | None
    value_labels: dict | None = None  # TODO: may consider alternative to dict/json at some point
