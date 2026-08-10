import sys
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import ClassVar
import duckdb
import pytest
from data_pipeline.metadata import RAW_DATA_FILE_TYPES
from data_pipeline.metadata import FileMeta
from data_pipeline.metadata import collect_file_info
from data_pipeline.metadata import create_manifest
from data_pipeline.metadata import extract_ref_period
from data_pipeline.metadata import extract_version
from data_pipeline.metadata import filter_list


@pytest.fixture
def db_file(tmp_path: Path) -> Path:
    """Provide fixture for database."""
    db_file = tmp_path / "test_db.duckdb"
    con = duckdb.connect(db_file)
    con.close()
    return db_file


@pytest.fixture
def file_metadata() -> Sequence[FileMeta]:
    """Sample file metadata for writing to db."""
    record0 = FileMeta(
        source_path=Path("data0/"),
        source_filename=Path("file0.dta"),
        read_access=False,
        last_modified=datetime(2013, 2, 15, tzinfo=UTC),
        file_size_bytes=120,
        ref_period=datetime(2012, 1, 1, tzinfo=UTC),
        version=2,
    )
    record1 = FileMeta(
        source_path=Path("data1/"),
        source_filename=Path("file1.sav"),
        read_access=True,
        last_modified=datetime(2017, 2, 15, tzinfo=UTC),
        file_size_bytes=5000,
        ref_period=datetime(2015, 1, 1, tzinfo=UTC),
        version=4,
    )

    return [record0, record1]


def test_filter_list():
    """Test filter_list."""
    list_in = ["some_dir", "another_dir", "Maatwerk", "geconverteerde Daten"]
    expected = ["some_dir", "another_dir"]
    output = filter_list(list_in, ["Maatwerk", "geconverteerde"])
    assert set(expected) == set(output)


class TestCollectFileInfo:
    """Test metadata collection."""

    path_list: ClassVar = [
        Path("data0/file_a.sav"),
        Path("data0/file_b.dta"),
        Path("data1/file_c.sav"),
        Path("data1/file_d.sas7bdat"),
        Path("data1/file_e.xlsx"),
    ]
    paths_without_access: ClassVar = [Path("data1/file_c.sav")]

    @pytest.fixture(scope="session")
    def data_source_dir(self, tmp_path_factory: pytest.TempPathFactory) -> Path:
        """Fixture for data source directory.

        Creates root directory with subdirectories and files with and
        without read permissions.
        """
        root_dir = tmp_path_factory.mktemp(Path(__file__).stem)

        for path in self.path_list:
            filepath, file = (root_dir / path).parent, path.name
            filepath.mkdir(exist_ok=True)
            (filepath / file).write_text("hello")
            if path in self.paths_without_access:
                # https://github.com/pytest-dev/pytest/issues/10679
                (filepath / file).chmod(0o066)
                # Two differences relative to CBS environment:
                # (1) Here we change *owner* permissions while
                # on CBS, users face *group* permissions.
                # (2) Permission systems on POSIX and Windows differ
                # and it's not worth simulating an entire Windows
                # permission system for this test.

        return root_dir

    def test_metadata(self, data_source_dir: Path) -> None:
        """Test reading of file metadata.

        Only test things that are not already covered in other unit tests
        and/or by the type checker.
        """
        file_metadata = collect_file_info(data_source_dir)

        expected_time_bound = datetime.now(UTC)
        for file in file_metadata:
            assert file.last_modified.tzinfo == UTC, "Wrong time zone"
            assert file.last_modified <= expected_time_bound, "Wrong access times"

        expected_metadata = [
            (data_source_dir / x.parent, Path(x.name)) for x in self.path_list if x.suffix in RAW_DATA_FILE_TYPES
        ]
        assert len(file_metadata) == len(expected_metadata), "Incorrect number of files parsed."

        parsed_metadata = [(f.source_path, f.source_filename) for f in file_metadata]

        assert set(parsed_metadata) == set(expected_metadata)

    @pytest.mark.xfail(sys.platform == "win32", reason="tests cannot manipulate ACLs on windows")
    def test_read_access(self, data_source_dir: Path) -> None:
        """Test read access is parsed correctly."""
        file_metadata = collect_file_info(data_source_dir)
        expected_metadata = [
            (data_source_dir / x.parent, Path(x.name), x not in self.paths_without_access)
            for x in self.path_list
            if x.suffix in RAW_DATA_FILE_TYPES
        ]
        parsed_metadata = [(f.source_path, f.source_filename, f.read_access) for f in file_metadata]

        assert set(parsed_metadata) == set(expected_metadata)


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("INPATAB2015V3.sav", datetime(2015, 1, 1, tzinfo=UTC)),
        ("SPOLIS200905V12.sav", datetime(2009, 5, 1, tzinfo=UTC)),
        ("SPOLIS200913V2.sav", None),
    ],
)
def test_extract_ref_period(test_input: str, expected: datetime | None):
    """Test extract_ref_period."""
    result = extract_ref_period(test_input)
    assert result == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("INPATAB2015V3.sav", 3),
        ("SPOLIS200905V12.sav", 12),
        ("SPOLIS200913V2.sav", 2),
        ("SPOLIS2009VX.sav", None),
    ],
)
def test_extract_version(test_input: str, expected: int | None):
    """Test extract_version."""
    result = extract_version(test_input)
    assert result == expected


def test_create_manifest(db_file: Path | str, file_metadata: Sequence[FileMeta]) -> None:
    """Test manifest creation."""
    create_manifest(file_metadata, db_file)

    with duckdb.connect(db_file) as con:
        expected_name = "source_manifest"
        count = con.execute(f"SELECT COUNT(*) FROM {expected_name}").fetchone()[0]  # noqa: S608
        assert count == len(file_metadata), "Incorrect number of rows"
