"""Module to read metadata on files of raw data in a location.

Assumptions
- All raw data are in one root directory.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
from data_pipeline.duckdb_schema import DuckDBSchema
from data_pipeline.utils import filter_list
from data_pipeline.utils import query_params

RAW_DATA_FILE_TYPES = [".dta", ".sav", ".sas7bdat"]

# AI NOTE: regex built by LLM.


def extract_ref_period(x: str) -> datetime | None:
    """Extract reference period from filename string.

    Returns
    -------
    A datetime object or None.

    Details
    -------
    A valid substring is either a 4-digit number, or a 6-digit
    number where the last 2 digits are are between 01 and 12.

    For valid substrings, the reference period is coded
    as the start day year-month combination.
    For invalid substrings, None is returned.
    """
    match = re.search(r"(\d{4})(\d{2})?V\d+(?:\.[^.]+)?$", x)
    first_month = 1
    last_month = 12
    if not match:
        return None
    year = int(match.group(1))
    month_str = match.group(2)
    if month_str is None:
        return datetime(year, 1, 1, tzinfo=UTC)
    month = int(month_str)
    if month < first_month or month > last_month:
        return None
    return datetime(year, month, 1, tzinfo=UTC)


def extract_version(x: str) -> int | None:
    """Extract version from filename string."""
    match = re.search(r"V(\d+)(?:\.[^.]+)?$", x)
    if not match:
        return None
    return int(match.group(1))


@dataclass
class FileMeta(DuckDBSchema):
    """Container for file metadata."""

    source_path: Path
    source_filename: Path
    read_access: bool
    last_modified: datetime
    file_size_bytes: int
    ref_period: datetime | None
    version: int | None
    bronze_path: Path | None = None
    schema_hash: str | None = None
    ingested_at: datetime | None = None


def collect_file_info(root_dir: Path | str, exclude_dir: list | None = None) -> Sequence[FileMeta]:
    """Collect info on files in a root directory.

    Arguments
    ---------
    root_dir: root directory to traverse, given as absolute path.
    exclude_dir: List of strings indicating directory names with matching
    substrings to drop.

    Returns
    -------
    A list of records, where each record is a FileMeta container
    of metadata.

    Notes
    -----
    This assumes the account running this function represents the users accessing
    the data later on.
    """
    data: list[FileMeta] = []
    for root, dirs, files in Path(root_dir).walk():
        if exclude_dir is not None:
            dirs[:] = filter_list(dirs, exclude_dir)

        files[:] = [f for f in files if Path(f).suffix in RAW_DATA_FILE_TYPES]

        for file in files:
            full_path = root / file
            file_stats = full_path.stat()

            record = FileMeta(
                source_filename=Path(file),
                source_path=root,
                ref_period=extract_ref_period(file),
                version=extract_version(file),
                read_access=check_read_access(full_path),
                last_modified=datetime.fromtimestamp(file_stats.st_mtime, tz=UTC),
                file_size_bytes=file_stats.st_size,
            )
            data.append(record)

    return data


def check_read_access(filepath: Path) -> bool:
    """Check if user has read access to `filepath`."""
    try:
        with filepath.open("rb"):
            return True
    except PermissionError:
        return False


def create_manifest(file_metadata: Sequence[FileMeta], db_file: Path | str) -> None:
    """Initialize 'manifest', a table with metadata about the source files.

    Arguments
    ---------
    file_metadata: list with metadata on files in raw storage.
    db_file: path to the database.

    """
    if len(file_metadata) == 0:
        return

    file_metadata[0].create_table("source_manifest", db_file)

    with duckdb.connect(db_file) as con:
        values_to_insert = [meta.as_dict().values() for meta in file_metadata]
        insert_params = query_params(list(values_to_insert[0]))
        con.executemany(f"INSERT INTO source_manifest VALUES ({insert_params})", values_to_insert)  # noqa: S608
