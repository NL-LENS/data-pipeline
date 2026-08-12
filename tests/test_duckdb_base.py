from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
import pytest
from data_pipeline.duckdb_base import DuckDBBigInt
from data_pipeline.duckdb_base import DuckDBDouble
from data_pipeline.duckdb_base import DuckDBRecord
from data_pipeline.duckdb_base import DuckDBTable
from data_pipeline.duckdb_base import ForeignKey

# TODO: use pytest magic to avoid repeated setup and speed up tests
# TODO: test with multiple primary keys?


@dataclass
class RecordForTesting(DuckDBRecord):
    """Record for testing."""

    path_name: Path = field(metadata={"primary_key": True})
    ref_period: datetime
    int_column: int | None = None


@dataclass
class TableForTesting(DuckDBTable):
    """Table for testing."""

    table_name = "test_table"


@pytest.fixture
def record() -> RecordForTesting:
    """Fixture for schema object."""
    time_now = datetime.now(UTC)
    return RecordForTesting(Path("/some/path"), time_now, 3)


@pytest.fixture
def db_table(tmp_path: Path) -> TableForTesting:
    """Sample table instance."""
    db_file = tmp_path / "test.db"
    return TableForTesting(db_file=db_file)


class TestDuckDBRecord:
    """Test DuckDBSchema."""

    def test_init(self, record: RecordForTesting) -> None:
        """Test initialization."""
        assert hasattr(record, "field_to_type_map"), "does not have field_to_type_map attr."
        expected_keys = [f.name for f in fields(record)]
        assert set(expected_keys) == set(record.field_to_type_map), "field_to_type_map has wrong keys."

    def test_as_dict(self, record: RecordForTesting) -> None:
        """Test as_dict method."""
        instance_dict = record.as_dict()
        expected_keys = [f.name for f in fields(record)]
        assert set(expected_keys) == set(instance_dict.keys())
        assert instance_dict["path_name"] == str(Path("/some/path"))  # Windows

    # TODO: add test that mapping is in correct order, or rather that
    # the mapping's order match the order in as_dict

    def test_invalid_schema(self) -> None:
        """Test invalid schema raises error."""

        @dataclass
        class InvalidRecord(DuckDBRecord):
            none_column: None = None

        with pytest.raises(TypeError, match="for field none_column"):
            _ = InvalidRecord(None)

    # TODO: test with other records, such as big int etc
    def test_from_table(self, db_table: TableForTesting, record: RecordForTesting) -> None:
        """Test from_table."""
        db_table.create(record.field_to_type_map, record.primary_key())
        db_table.insert(record)

        new_instance = RecordForTesting.from_table(lookup={"path_name": Path("/some/path")}, table=db_table)
        # NOTE: the types of the key values are not validated here.
        assert record == new_instance, "Incorrectly constructing record from table."

    def test_from_table_invalid_keys_raises(self, db_table: TableForTesting) -> None:
        """Test that error is raised."""
        wrong_keys = {"int_column": 5}
        with pytest.raises(RuntimeError):
            RecordForTesting.from_table(wrong_keys, db_table)


class TestDuckDBTable:
    """Test TableForTesting."""

    def test_build_ddl(self, db_table: TableForTesting, record: RecordForTesting) -> None:
        """Test build_ddl."""
        ddl = db_table.build_ddl(record.field_to_type_map, record.primary_key())
        expected_ddl = (
            """CREATE TABLE IF NOT EXISTS "test_table" """
            """("path_name" VARCHAR, "ref_period" TIMESTAMPTZ, "int_column" INTEGER, """
            """PRIMARY KEY ("path_name"))"""
        )
        assert ddl == expected_ddl, "Incorrect ddl created."

    def test_create(self, db_table: TableForTesting, record: RecordForTesting) -> None:
        """Test table creation."""
        db_table.create(record.field_to_type_map, record.primary_key())

        with duckdb.connect(db_table.db_file) as con:
            sql = """
               SELECT column_name
               FROM information_schema.columns
               WHERE table_name = ?
            """
            columns = con.execute(sql, (db_table.table_name,)).fetchall()
            columns = [x[0] for x in columns]
            assert columns == [f.name for f in fields(record)], "Table does not exist with correct columns."

    def test_insert(self, record: RecordForTesting, db_table: TableForTesting) -> None:
        """Test writing to database."""
        db_table.create_from_record(record)
        db_table.insert(record)
        with duckdb.connect(db_table.db_file) as con:
            con.sql("SET TIMEZONE='UTC'")  # Need UTC-awareness per connection
            # Work-around: querying TZ-aware columns with con.execute().fetch*
            # fails b/c of a missing (and outdated) runtime dep.
            # https://github.com/duckdb/duckdb-python/discussions/406
            # Querying with polars `.pl().rows()` does not work on windows; requires
            # tzdata package
            non_time_data = con.sql("SELECT * EXCLUDE ref_period FROM test_table").fetchall()
            time_query = (
                "SELECT YEAR(ref_period)",
                "MONTH(ref_period)",
                "DAY(ref_period)",
                "HOUR(ref_period)",
                "MINUTE(ref_period)",
                "FROM test_table",
            )
            time_data = con.sql(", ".join(time_query)).fetchall()

        assert len(non_time_data) == 1, "Incorrect number of rows"

        expected_non_time_data = {field_: value for field_, value in record.as_dict().items() if field_ != "ref_period"}

        assert non_time_data[0] == tuple(expected_non_time_data.values()), (
            "Round-trip py-DuckDB-py fails for non-time data."
        )

        expected_time_data = (
            record.ref_period.year,
            record.ref_period.month,
            record.ref_period.day,
            record.ref_period.hour,
            record.ref_period.minute,
        )
        assert time_data[0] == expected_time_data, "Round-trip py-DuckDB-py fails for time data"

    def test_read(self, record: RecordForTesting, db_table: TableForTesting) -> None:
        """Test reading a single record from the table."""
        db_table.create_from_record(record)
        db_table.insert(record)

        key_dict = {"path_name": str(record.path_name)}
        data = db_table.read(key_dict, column_types=record.py_types())

        for field_ in fields(record):
            assert getattr(record, field_.name) == data[field_.name], "Round-trip fails for field {field_}"

    def test_read_missing_raises(self, db_table: TableForTesting, record: RecordForTesting) -> None:
        """Test that reading record from table that is missing raises error."""
        db_table.create_from_record(record)
        db_table.insert(record)

        key_dict = {"path_name": "/wrong/path"}
        with pytest.raises(RuntimeError):
            _ = db_table.read(key_dict, column_types=RecordForTesting.py_types())

    def test_64bit_numeric(self, db_table: TableForTesting) -> None:
        """Test for 64bit numeric data types in DuckDB."""

        @dataclass
        class BigRecord(DuckDBRecord):
            """Schema for testing."""

            int_column: DuckDBBigInt
            double_column: DuckDBDouble
            optional_bigint: DuckDBBigInt | None = None

        record = BigRecord(int_column=DuckDBBigInt(3), double_column=DuckDBDouble(10.3), optional_bigint=None)
        db_table.create_from_record(record)
        db_table.insert(record)
        with duckdb.connect(db_table.db_file) as con:
            data = con.sql("SELECT * FROM test_table").fetchall()[0]

        assert data == tuple(record.as_dict().values()), "Round-trip fails for 64bit precision types."

    def test_update(self, db_table: TableForTesting, record: RecordForTesting) -> None:
        """Test update."""
        db_table.create_from_record(record)
        db_table.insert(record)

        record.int_column = 100
        record.ref_period = datetime.now(UTC)

        db_table.update(record)

        new_record = RecordForTesting.from_table(lookup=record.key_attributes, table=db_table)
        assert new_record == record, "Updating record fails."


class TestDuckDBTableWithForeignKeys:
    """Test table with foreign keys."""

    @dataclass
    class ForeignKeyRecord(DuckDBRecord):
        """Record with foreign key."""

        id: int = field(metadata={"foreign_key": {"table": "table1", "column": "id"}})
        address: str

    @dataclass
    class PrimaryKeyRecord(DuckDBRecord):
        """Record with primary key."""

        id: int = field(metadata={"primary_key": True})
        name: str

    @dataclass
    class ForeignKeyTable(DuckDBTable):
        """Table with records with foreign keys."""

        table_name = "table2"
        foreign_key_table = "table1"

    @dataclass
    class PrimaryKeyTable(DuckDBTable):
        """Table with primary keys."""

        table_name = "table1"

    @pytest.fixture
    def db_file(self, tmp_path: Path) -> Path:
        """Database file for tests."""
        return tmp_path / "test.db"

    @pytest.fixture
    def table1(self, db_file: Path) -> PrimaryKeyTable:
        """Fixture primary key table."""
        record1 = self.PrimaryKeyRecord(1, "Joe")
        record2 = self.PrimaryKeyRecord(2, "Jane")

        table = self.PrimaryKeyTable(db_file)
        table.create_from_record(record1)
        table.insert_many([record1, record2])

        return table

    def test_build_ddl(self, db_file: Path):
        """Test that ddl is created correctly."""
        record = self.ForeignKeyRecord(1, "Amsterdam")
        fk_table = self.ForeignKeyTable(db_file)
        ddl = fk_table.build_ddl(record.field_to_type_map, foreign_key=record.foreign_key())

        expected_ddl = (
            f"""CREATE TABLE IF NOT EXISTS "{fk_table.table_name}" """
            f"""("id" INTEGER, "address" VARCHAR, """
            f"""FOREIGN KEY (id) REFERENCES table1(id))"""
        )
        assert ddl == expected_ddl, "Incorrect ddl created."

    @pytest.mark.usefixtures("table1")
    def test_create(self, db_file: Path):
        """Test that table with foreign key can be created."""
        record = self.ForeignKeyRecord(1, "Amsterdam")
        fk_table = self.ForeignKeyTable(db_file)
        fk_table.create_from_record(record)

        with duckdb.connect(db_file) as con:
            sql = """
               SELECT column_name
               FROM information_schema.columns
               WHERE table_name = ?
            """
            columns = con.execute(sql, (fk_table.table_name,)).fetchall()
            columns = [x[0] for x in columns]
            assert columns == [f.name for f in fields(record)], "Table does not exist with correct columns."

            sql = """
               SELECT constraint_text
               FROM information_schema.constraint_column_usage
               WHERE table_name = ?
            """
            constraints = con.execute(sql, (fk_table.table_name,)).fetchall()
            assert len(constraints) == 1, "Table constraint not created."


class TestForeignKey:
    """Test build_fk_instance."""

    def test_from_dict(self) -> None:
        """Test from_dict."""
        fk_dict = {"a": {"table": "t3", "column": "id"}, "b": {"table": "t3", "column": "j"}}
        instance = ForeignKey.from_dict(fk_dict)
        assert instance.table == "t3"
        assert instance.self_cols == ("a", "b")
        assert instance.ref_cols == ("id", "j")

    def test_build_fk_ddl(self) -> None:
        """Test that DDL is build correctly."""
        fk_dict = {"t3_id": {"table": "t3", "column": "id"}, "t3_j": {"table": "t3", "column": "j"}}
        instance = ForeignKey.from_dict(fk_dict)
        ddl = instance.build_fk_ddl()
        expected_ddl = "FOREIGN KEY (t3_id, t3_j) REFERENCES t3(id, j)"
        assert ddl == expected_ddl, "Incorrect DDL created."

    def test_from_dict_raises(self):
        """Test that error is raised when multiple tables are specified."""
        fk_dict = {"id": {"table": "t2", "column": "id"}, "j": {"table": "t3", "column": "j"}}

        with pytest.raises(RuntimeError):
            _ = ForeignKey.from_dict(fk_dict)
