import typing
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest import mock
import duckdb
import numpy as np
import polars as pl
import pyreadstat
import pytest
from polars.testing import assert_frame_equal
from data_pipeline.bronze import ingest_source
from data_pipeline.bronze import read_table_meta_to_db
from data_pipeline.bronze import stream_to_bronze
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SourceManifest


@pytest.fixture
def random_dates(rng: np.random._generator.Generator | None = None):
    """Generate a set of random dates."""
    size = 100_000
    if rng is None:
        rng = np.random.default_rng()
    start_date = np.datetime64("2000-02-13")
    end_date = np.datetime64("2025-12-25")
    day_range = end_date - start_date
    dates = np.repeat(start_date, size)
    deltas = rng.integers(1, day_range.item().days, size)
    deltas = np.array(deltas, dtype=np.timedelta64)
    return dates + deltas


class TestBronze:
    """Test conversion from source to bronze."""

    sample_size: int = 100_000
    rng: np.random._generator.Generator = np.random.default_rng()
    file_in: Path = Path("file_in.sav")
    missing_file: Path = Path("missing_file.sav")
    file_out: str = "file_out.parquet"
    size_file_in: int = 5
    sav_column_labels: typing.ClassVar[dict[str, str]] = {
        "person_id": "Person identifier",
        "date": "Reference date of the event",
        "wage": "Hourly wage",
        "sector": "Economic sector",
        "null_col": "Column added later to the schema.",
    }

    @pytest.fixture
    def db_file(self, tmp_path: Path) -> Path:
        """File for metadata database."""
        return tmp_path / "metadata.db"

    @pytest.fixture
    def sav_file(self, tmp_path: Path) -> Path:
        """Path to sav file."""
        return tmp_path / self.file_in

    @pytest.fixture
    def metadata_db(self, db_file: Path, tmp_path: Path):
        """Set up database with file data."""
        file_record1 = FileMetaRecord(
            source_path=tmp_path,
            source_filename=self.file_in,
            read_access=True,
            last_modified=datetime(2025, 4, 25, tzinfo=UTC),
            file_size_bytes=self.size_file_in,
            ref_period=None,
            version=None,
        )
        file_record2 = FileMetaRecord(
            source_path=tmp_path,
            source_filename=self.missing_file,
            read_access=True,
            last_modified=datetime(2023, 10, 15, tzinfo=UTC),
            file_size_bytes=10,
            ref_period=None,
            version=None,
        )
        table = SourceManifest(db_file=db_file)
        table.create_from_record(file_record1)
        table.insert_many([file_record1, file_record2])

    def create_random_dates(self):
        """Generate a set of random dates."""
        start_date = np.datetime64("2000-02-13")
        end_date = np.datetime64("2025-12-25")
        day_range = (end_date - start_date).item().days

        dates = np.repeat(start_date, self.sample_size)
        int_deltas = self.rng.integers(1, day_range, self.sample_size)
        time_deltas = np.array(int_deltas, dtype=np.timedelta64)

        return dates + time_deltas

    @pytest.fixture
    def sav_test_data(self, sav_file: Path) -> pl.DataFrame:
        """Create .sav and return corresponding dataframe.

        The dataframe has column types that are reverse-engineered from SPOLIS and INPATAB examples.

        Covers the most general case that the tool currently handles.
        User missing values are coded as "Null" in the .sav file, and specified
        via the missing_ranges, see for details:
            https://github.com/Roche/pyreadstat#missing-values for user-defined mising values.

        For numeric values, a value label can also indicate a missing value, but they
        are stored as-is in the .sav file.
        """
        # TODO: try with other numeric types? float32, int32, in64?

        data_dict = {
            "person_id": self.rng.integers(1, 1_000_000_000, size=self.sample_size).astype(
                str
            ),  # person IDs are coded as strings
            "date": self.create_random_dates().astype(str),  # sav files store dates as str
            "wage": self.rng.random(size=self.sample_size).astype(np.float64),
            "sector": self.rng.choice(["1", "2", "3"], size=self.sample_size),
            "null_col": np.array([None] * self.sample_size, dtype=np.float64),
        }
        # NOTE/TODO: types when writing to sav are complicated
        # See here https://github.com/Roche/pyreadstat#variable-type-conversion
        # Q: When reading real-world files, are they all floats and not int?
        # Check in CBS and update test as necessary
        # According to events_bottom_up, we need to deal with string, float64 and 32, int 8 to 32
        # but probably should be int 16 to 64?
        variable_value_labels = {
            "sector": {"1": "sector a", "2": "sector b", "3": "sector c", "99": "missing"},
            "wage": {9999999999: "missing", 9999999998: "also missing"},
        }
        user_missing_ranges = {
            "sector": [{"hi": "98", "lo": "98"}],
            "wage": [{"lo": 100, "hi": 110}],  # 110 is included upper bound
        }

        possible_missing_values = {
            "sector": ["99", "98"],
            "wage": [9999999999, 9999999998, *(np.arange(100, 111))],  # allow 110 for the included upper bound
        }

        for column, values in possible_missing_values.items():
            n_missing = 500
            missing_idx = self.rng.integers(0, self.sample_size - 1, n_missing)
            data_dict[column][missing_idx] = self.rng.choice(values, n_missing)

        data = pl.DataFrame(data_dict)
        pyreadstat.write_sav(
            data,
            str(sav_file),
            column_labels=self.sav_column_labels,
            variable_value_labels=variable_value_labels,
            missing_ranges=user_missing_ranges,
        )

        # when sav data are read with user_missing = True, the columns
        # defined in missing_ranges are automatically converted to Null
        # and meta.user_missing is an empty dictionary.
        # We add this example here for completeness, but the assumed workflow
        # is to read with user_missing = False and ignore `meta.missing_ranges`
        return data.with_columns(
            pl.when(pl.col("sector") == "y").then(None).otherwise(pl.col("sector")).alias("sector"),
            pl.when(pl.col("wage").is_in(np.arange(100, 111))).then(None).otherwise(pl.col("wage")).alias("wage"),
        )

    def test_stream_to_bronze(
        self,
        sav_test_data: tuple[pl.DataFrame, Path],
        sav_file: Path,
        tmp_path: Path,
    ) -> None:
        """Test stream_to_bronze function."""
        expected_data = sav_test_data

        # The null_col is completely missing in the underlying .sav file, and
        # read as pl.Null column
        expected_data = expected_data.with_columns(pl.lit(None).alias("null_col"))
        file_path_out = tmp_path / self.file_out
        stream_to_bronze(sav_file, file_path_out, chunk_size=9_999)

        result = pl.read_parquet(file_path_out)
        (
            assert_frame_equal(
                expected_data,
                result,
            ),
            "Original data is not equal the processed data.",
        )

    @pytest.mark.usefixtures("metadata_db")
    @pytest.mark.usefixtures("sav_test_data")
    def test_read_table_meta_to_db(self, sav_file: Path, db_file: Path) -> None:
        """Minimal test that .sav file metadata are correctly written to the database."""
        read_table_meta_to_db(db_file, source_path=sav_file.parent, source_filename=sav_file.name)

        with duckdb.connect(db_file) as con:
            data = con.sql("SELECT variable, description from sav_meta").fetchall()

        assert len(data) == len(self.sav_column_labels), "Incorrect number of variables recorded."
        expected_data = list(self.sav_column_labels.items())
        assert data == expected_data, "Variable name and description incorrectly recorded."

    @pytest.mark.usefixtures("metadata_db")
    @pytest.mark.usefixtures("sav_test_data")
    @mock.patch("data_pipeline.bronze.stream_to_bronze")
    @mock.patch("data_pipeline.bronze.read_table_meta_to_db")
    def test_ingest_source(
        self, mock_stream_to_bronze: mock.Mock, mock_read_table_meta_to_db: mock.Mock, db_file: Path, tmp_path: Path
    ) -> None:
        """Test ingest_source function."""
        dest_file = tmp_path / self.file_out
        ingest_source(db_file=db_file, source_path=tmp_path, source_filename=self.file_in, dest_file=dest_file)

        source_manifest = SourceManifest(db_file=db_file)
        lookup = {"source_path": tmp_path, "source_filename": self.file_in}
        file_meta_record = FileMetaRecord.from_table(table=source_manifest, lookup=lookup)

        assert file_meta_record.bronze_path == dest_file, "Bronze path not recorded."
        assert file_meta_record.file_size_bytes > self.size_file_in, "File size not updated."
        assert file_meta_record.version is None, "version modified when it should not."
        assert file_meta_record.ref_period is None, "ref period modified when it should not."

        mock_stream_to_bronze.assert_called_once()
        mock_read_table_meta_to_db.assert_called_once()
