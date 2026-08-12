"""Module to read metadata on files of raw data in a location.

Assumptions
- All raw data are in one root directory.
"""

import re
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from pathlib import Path
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SourceManifest
from data_pipeline.utils import filter_list

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
