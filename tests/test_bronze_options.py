"""Regression tests for chunking, benchmarks and manifest selection."""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import polars as pl
import pyreadstat
import pytest
from polars.testing import assert_frame_equal
from data_pipeline import bronze
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SourceManifest


def test_benchmark_stops_before_reading_another_chunk(tmp_path: Path):
    """Do not time an unused extra read or overwrite the production parquet."""
    source = tmp_path / "source.sav"
    dest = tmp_path / "source.parquet"
    data = pl.DataFrame({"value": list(range(100))})
    pyreadstat.write_sav(data, source)
    original = b"existing output"
    dest.write_bytes(original)

    def chunks() -> Iterator[pl.DataFrame]:
        yield data.head(10).cast({"value": pl.Float64})
        pytest.fail("Benchmark read beyond the 10% boundary")

    with mock.patch.object(bronze, "iter_sav_chunks", return_value=chunks()):
        bronze.stream_to_bronze(source, dest, chunk_size=10, num_processes=1, benchmark=True)
    assert dest.read_bytes() == original
    result = pl.read_parquet(tmp_path / "source.benchmark.parquet")
    assert_frame_equal(result, data.head(10).cast({"value": pl.Float64}).rename({"value": "VALUE"}))


def test_benchmark_preserves_metadata(tmp_path: Path):
    """A sample is not a completed ingestion and does not add column metadata."""
    db_file = tmp_path / "meta.duckdb"
    record = FileMetaRecord(
        source_path=tmp_path,
        source_filename=Path("source.sav"),
        read_access=True,
        last_modified=datetime(2025, 1, 1, tzinfo=UTC),
        file_size=1,
        ref_period=None,
        version=None,
        bronze_path=tmp_path / "existing.parquet",
        ingested_at=datetime(2025, 2, 1, tzinfo=UTC),
    )
    manifest = SourceManifest(db_file)
    manifest.create_from_record(record)
    manifest.insert_many([record])
    with (
        mock.patch.object(bronze, "stream_to_bronze") as stream,
        mock.patch.object(bronze, "write_column_metadata_to_db") as columns,
    ):
        bronze.ingest_source(manifest, tmp_path, record.source_filename, tmp_path / "out.parquet", benchmark=True)
    stream.assert_called_once()
    columns.assert_not_called()
    actual = FileMetaRecord.from_table(
        lookup={"source_path": tmp_path, "source_filename": record.source_filename},
        table=manifest,
    )
    assert actual == record


def test_build_filters_read_access_and_period(tmp_path: Path):
    """Skip inaccessible, undated, unmatched and out-of-range sources."""
    db_file = tmp_path / "meta.duckdb"
    records = [
        FileMetaRecord(
            source_path=tmp_path,
            source_filename=Path(name),
            read_access=readable,
            last_modified=datetime(2025, 1, 1, tzinfo=UTC),
            file_size=1,
            ref_period=period,
            version=1,
        )
        for name, readable, period in [
            ("match_start.sav", True, datetime(2020, 1, 1, tzinfo=UTC)),
            ("match_end.sav", True, datetime(2021, 12, 31, tzinfo=UTC)),
            ("match_denied.sav", False, datetime(2020, 1, 1, tzinfo=UTC)),
            ("match_undated.sav", True, None),
            ("match_old.sav", True, datetime(2019, 1, 1, tzinfo=UTC)),
            ("other.sav", True, datetime(2020, 1, 1, tzinfo=UTC)),
        ]
    ]
    manifest = SourceManifest(db_file)
    manifest.create_from_record(records[0])
    manifest.insert_many(records)
    dest = tmp_path / "nested" / "bronze"
    with mock.patch.object(bronze, "ingest_source") as ingest:
        bronze.build(db_file, "match", 2020, 2021, dest, benchmark=True, offset_workaround=True)
    assert dest.is_dir()
    assert {call.args[2].name for call in ingest.call_args_list} == {"match_start.sav", "match_end.sav"}
    for call in ingest.call_args_list:
        assert call.args[4] is None
        assert call.kwargs == {"benchmark": True, "offset_workaround": True}
    empty_dest = tmp_path / "empty"
    bronze.build(db_file, "absent", 2020, 2021, empty_dest)
    assert not empty_dest.exists()


def test_offset_reader_advances_by_actual_rows():
    """Short reads must not skip source rows; the last request is bounded."""
    chunks = [pl.DataFrame({"x": [1, 2]}), pl.DataFrame({"x": [3, 4, 5]})]
    with (
        mock.patch.object(bronze, "read_sav_meta", return_value=SimpleNamespace(number_rows=5)),
        mock.patch.object(
            bronze.pyreadstat, "read_file_multiprocessing", side_effect=[(c, None) for c in chunks]
        ) as read,
    ):
        actual = list(bronze.iter_sav_chunks("source.sav", chunk_size=3, num_processes=1, offset_workaround=True))
    assert pl.concat(actual)["x"].to_list() == [1, 2, 3, 4, 5]
    assert [call.kwargs["row_offset"] for call in read.call_args_list] == [0, 2]
    assert [call.kwargs["row_limit"] for call in read.call_args_list] == [3, 3]


def test_offset_reader_rejects_empty_chunk():
    """Unexpected empty reads must fail instead of looping forever."""
    with (
        mock.patch.object(bronze, "read_sav_meta", return_value=SimpleNamespace(number_rows=5)),
        mock.patch.object(bronze.pyreadstat, "read_file_multiprocessing", return_value=(pl.DataFrame(), None)),
        pytest.raises(RuntimeError, match="returned no rows"),
    ):
        list(bronze.iter_sav_chunks("source.sav", offset_workaround=True))


@pytest.mark.parametrize("offset_workaround", [False, True])
def test_empty_source(tmp_path: Path, offset_workaround: bool):
    """Empty SAV files produce an empty parquet without dividing by zero."""
    source = tmp_path / "empty.sav"
    pyreadstat.write_sav(pl.DataFrame({"value": pl.Series([], dtype=pl.Float64)}), source)
    dest = tmp_path / "empty.parquet"
    bronze.stream_to_bronze(
        source,
        dest,
        chunk_size=bronze.DEFAULT_CHUNKSIZE,
        num_processes=1,
        offset_workaround=offset_workaround,
    )
    assert pl.read_parquet(dest).height == 0
