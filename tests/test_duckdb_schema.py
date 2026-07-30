from dataclasses import dataclass
from dataclasses import fields
from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
import pytest
from data_pipeline.duckdb_schema import DuckDBBigInt
from data_pipeline.duckdb_schema import DuckDBDouble
from data_pipeline.duckdb_schema import DuckDBSchema


@dataclass
class SchemaForTesting(DuckDBSchema):
    """Schema for testing."""

    path_name: Path
    ref_period: datetime
    int_column: int | None = None


@pytest.fixture
def schema_instance() -> SchemaForTesting:
    """Fixture for schema object."""
    time_now = datetime.now(UTC)
    return SchemaForTesting(Path("/some/path"), time_now, 3)


class TestDuckDBSchema:
    """Test DuckDBSchema."""

    def test_init(self, schema_instance: SchemaForTesting) -> None:
        """Test initialization."""
        assert hasattr(schema_instance, "field_to_type_map"), "does not have field_to_type_map attr."
        expected_keys = [f.name for f in fields(schema_instance)]
        assert set(expected_keys) == set(schema_instance.field_to_type_map), "field_to_type_map has wrong keys."

    def test_as_dict(self, schema_instance: SchemaForTesting) -> None:
        """Test as_dict method."""
        instance_dict = schema_instance.as_dict()
        expected_keys = [f.name for f in fields(schema_instance)]
        assert set(expected_keys) == set(instance_dict.keys())
        assert instance_dict["path_name"] == str(Path("/some/path"))  # Windows

    def test_build_ddl(self, schema_instance: SchemaForTesting) -> None:
        """Test build_ddl."""
        ddl = schema_instance.build_ddl("test_table")
        expected_ddl = (
            """CREATE TABLE IF NOT EXISTS "test_table" """
            """("path_name" VARCHAR, "ref_period" TIMESTAMPTZ, "int_column" INTEGER)"""
        )
        assert ddl == expected_ddl, "Incorrect ddl created."

    def test_create_table(self, schema_instance: SchemaForTesting, tmp_path: Path) -> None:
        """Test table creation."""
        db_file = tmp_path / "test.db"
        schema_instance.create_table("test_table", db_file)
        # add test that table 'test' exists in table schema

    def test_write_to_db(self, schema_instance: SchemaForTesting, tmp_path: Path) -> None:
        """Test writing to database."""
        db_file = tmp_path / "test.db"
        schema_instance.create_table("test_table", db_file)
        schema_instance.write_to_db("test_table", db_file)
        with duckdb.connect(db_file) as con:
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

        expected_non_time_data = {
            field_: value for field_, value in schema_instance.as_dict().items() if field_ != "ref_period"
        }

        assert non_time_data[0] == tuple(expected_non_time_data.values()), (
            "Round-trip py-DuckDB-py fails for non-time data."
        )

        expected_time_data = (
            schema_instance.ref_period.year,
            schema_instance.ref_period.month,
            schema_instance.ref_period.day,
            schema_instance.ref_period.hour,
            schema_instance.ref_period.minute,
        )
        assert time_data[0] == expected_time_data, "Round-trip py-DuckDB-py fails for time data"

    def test_invalid_schema(self) -> None:
        """Test invalid schema raises error."""

        @dataclass
        class InvalidSchema(DuckDBSchema):
            none_column: None = None

        with pytest.raises(TypeError, match="for field none_column"):
            _ = InvalidSchema(None)

    def test_64bit_numeric(self, tmp_path: Path) -> None:
        """Test for 64bit numeric data types in DuckDB."""

        @dataclass
        class BigSchema(DuckDBSchema):
            """Schema for testing."""

            int_column: DuckDBBigInt
            double_column: DuckDBDouble

        instance = BigSchema(int_column=DuckDBBigInt(3), double_column=DuckDBDouble(10.3))
        db_file = tmp_path / "test.db"
        table_name = "test_table"
        instance.create_table(table_name, db_file)
        instance.write_to_db(table_name, db_file)
        with duckdb.connect(db_file) as con:
            data = con.sql("SELECT * FROM test_table").fetchall()[0]

        assert data == tuple(instance.as_dict().values()), "Round-trip fails for 64bit precision types."
