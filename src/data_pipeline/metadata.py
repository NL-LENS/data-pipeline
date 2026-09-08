"""Module to read metadata on files of raw data in a location.

The module assumes that all raw data are in one root directory.

Notes
-----
The functions `extract_ref_period` and `extract_version`
are closely linked: intended usage is that, from the same
input string, they extract a reference year (if present),
and a version number (if present). See the respective
examples and tests, for details.
"""

import logging
import re
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from pathlib import Path
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SourceManifest
from data_pipeline.utils import filter_list

RAW_DATA_FILE_TYPES = [".dta", ".sav", ".sas7bdat"]

# AI NOTE: regex built and updated by LLM.


def extract_ref_period(x: str) -> datetime | None:
    """Extract reference period from filename string.

    Arguments
    ---------
    x: A string, referring to a file name, including file ending.

    Returns
    -------
    A datetime object or None.

    Details
    -------
    A valid filename ends, before its extension, with a
    4-digit year, optionally followed by a 2-digit month
    between 01 and 12, optionally followed by other non-dot characters,
    then a version marker `V`, and one or more digits.

    For valid substrings, the reference period is coded
    as the start day year-month combination.
    For invalid substrings, None is returned.

    Examples
    --------
    >>> extract_ref_period("INPA2024TABV2.sav")
    >>> 2024
    >>> extract_version("INPATAB2024X.sav")
    >>> None # not followed by `V`
    >>> extract_version("SOMEBUSV20241.sav")
    >>> None # not followed by `V`
    """
    match = re.search(r"(\d{4})(\d{2})?[^.]*?V\d+(?:\.[^.]+)?$", x)
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
    """Extract version from filename string.

    Arguments
    ---------
    x: A string, referring to a file name, including file ending.

    Details
    -------
    This matches any numbers following a `V`, so also things like
    "V20241". It is unclear if this is intended behavior or not; depends
    on CBS naming conventions.

    Examples
    --------
    >>> extract_version("INPA2024TABV2.sav")
    >>> 2
    >>> extract_version("INPATAB2024X.sav")
    >>> None # No "V" followed by digit
    >>> extract_version("SOMEBUSV20241.sav")
    >>> 20241 # entire number after "V"
    """
    match = re.search(r"V(\d+)(?:\.[^.]+)?$", x)
    if not match:
        return None
    return int(match.group(1))


def collect_file_info(root_dir: Path | str, exclude_dir: list | None = None) -> Sequence[FileMetaRecord]:
    """Collect info on files in a root directory.

    Arguments
    ---------
    root_dir: root directory to traverse, given as absolute path.
    exclude_dir: List of strings indicating directory names with matching
    substrings to drop.

    Returns
    -------
    A list of records, where each record is a FileMetaRecord container
    of metadata.

    Notes
    -----
    This assumes the account running this function represents the users accessing
    the data later on.
    """
    data: list[FileMetaRecord] = []
    for root, dirs, files in Path(root_dir).walk():
        if exclude_dir is not None:
            dirs[:] = filter_list(dirs, exclude_dir)

        files[:] = [f for f in files if Path(f).suffix in RAW_DATA_FILE_TYPES]

        for file in files:
            full_path = root / file
            last_modified, file_size_bytes = get_file_stats(full_path)

            record = FileMetaRecord(
                source_filename=Path(file),
                source_path=root,
                ref_period=extract_ref_period(file),
                version=extract_version(file),
                read_access=check_read_access(full_path),
                last_modified=last_modified,
                file_size_bytes=file_size_bytes,
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


def get_file_stats(filepath: Path) -> tuple[datetime, int]:
    """File statistics from stat().

    Returns
    -------
    tuple: First entry is last modification time, second entry is file size.
    """
    file_stats = filepath.stat()
    last_modified = datetime.fromtimestamp(file_stats.st_mtime, tz=UTC)
    file_size_bytes = file_stats.st_size
    return (last_modified, file_size_bytes)


def create_manifest(file_metadata: Sequence[FileMetaRecord], db_file: Path | str) -> None:
    """Initialize 'manifest', a table with metadata about the source files.

    Arguments
    ---------
    file_metadata: list with metadata on files in raw storage.
    db_file: path to the database.

    """
    if len(file_metadata) == 0:
        return

    table = SourceManifest(db_file)
    table.create_from_record(file_metadata[0])
    table.insert_many(file_metadata)


def run_init(root_dir: Path, db_file: Path) -> None:
    """Parse file metadata and create database file."""
    logger = logging.getLogger(__name__)
    file_metadata = collect_file_info(root_dir)
    create_manifest(file_metadata, db_file)
    logger.info("Created %s with file metadata in %s", str(root_dir), str(db_file))
