from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
import numpy as np
import polars as pl
import pytest
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest
from data_pipeline.silver import NOTE_COLNAME
from data_pipeline.silver import PERSON_COLNAME
from data_pipeline.silver import TIME_COLNAME
from data_pipeline.silver import SilverConfig
from data_pipeline.silver import convert_to_silver
from .conftest import TestData


class TestSilver(TestData):
    """Class for testing bronze->silver conversion."""

    @pytest.fixture
    def input_paths(self, tmp_path: Path) -> dict[str, Path]:
        """Create dictionary with paths, used as inputs for tests."""
        return {
            "source_path": Path("/path/to/raw/data"),
            "source_filename": Path("raw_file.sav"),
            "bronze_file": tmp_path / "bronze.parquet",
        }

    @pytest.fixture
    def bronze_data(self, input_paths: dict[str, Path]) -> None:
        """Bronze data used is input for silver."""
        self.validity_mask = np.bool(np.ones(self.sample_size))

        """Create .parquet of bronze."""
        data_dict = {
            "RINPERSOON": self.make_identifiers(),
            "RINPERSOONS": self.make_categorical(["R"], [1]),
            "IRRELEVANT_ID": self.make_identifiers(),
            "TIME1": self.random_dates().astype(str),
            "TIME2": self.random_dates().astype(str),
            "CONTINUOUS_ATTR1": self.rng.random(size=self.sample_size).astype(np.float64),
            "CONTINUOUS_ATTR2": self.rng.random(size=self.sample_size).astype(np.float64),
            "CAT_ATTR1": self.make_categorical(["a", "b", "c", "missing"], [0.1, 0.6, 0.2, 0.1]),
            "CAT_ATTR2": self.make_categorical(["x", "y", "missing"], [0.2, 0.75, 0.05]),
        }

        data_dict["CONTINUOUS_ATTR1"] = self.corrupt(
            x=data_dict["CONTINUOUS_ATTR1"], inject=np.float64(99999), size=int(0.02 * self.sample_size)
        )

        data_dict["CONTINUOUS_ATTR2"] = self.corrupt(
            x=data_dict["CONTINUOUS_ATTR2"], inject=np.float64(99998), size=int(0.01 * self.sample_size)
        )
        data_dict["CONTINUOUS_ATTR2"] = self.corrupt(
            x=data_dict["CONTINUOUS_ATTR2"], inject=np.float64(99997), size=int(0.02 * self.sample_size)
        )

        data_dict["TIME1"] = self.corrupt(
            x=data_dict["TIME1"], inject="--------", size=int(0.02 * self.sample_size), valid=False
        )
        data_dict["RINPERSOONS"] = self.corrupt(
            x=data_dict["RINPERSOONS"], inject="not_R", size=int(0.05 * self.sample_size), valid=False
        )

        data = pl.DataFrame(data_dict)
        data.write_parquet(input_paths["bronze_file"])

    @pytest.fixture
    def metadata_db(self, db_file: Path, input_paths: dict[str, Path]) -> None:
        """Metadata corresponding to the bronze data."""
        source_path = input_paths["source_path"]
        source_filename = input_paths["source_filename"]

        file_record = FileMetaRecord(
            source_path=source_path,
            source_filename=source_filename,
            read_access=True,
            last_modified=datetime(2025, 4, 25, tzinfo=UTC),
            file_size=1_000,
            ref_period=None,
            version=None,
            bronze_path=input_paths["bronze_file"],
        )
        table = SourceManifest(db_file=db_file)
        table.create_from_record(file_record)
        table.insert(file_record)

        rinpersoon_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="RINPERSOON",
            original_variable="Rinpersoon",
            readstat_type="str",
            description="",
            value_labels=None,
        )

        rinpersoons_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="RINPERSOONS",
            original_variable="Rinpersoons",
            readstat_type="str",
            description="",
            value_labels={"R": "valid", "not_R": "not valid"},
        )

        irrelevant_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="IRRELEVANT_ID",
            original_variable="irrelevant_id",
            readstat_type="str",
            description="",
            value_labels=None,
        )

        time1_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="TIME1",
            original_variable="TIME1",
            readstat_type="str",
            description="",
            value_labels=None,
        )

        time2_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="TIME2",
            original_variable="TIME2",
            readstat_type="str",
            description="",
            value_labels=None,
        )

        time2_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="TIME2",
            original_variable="TIME2",
            readstat_type="str",
            description="",
            value_labels=None,
        )

        continuous1_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="CONTINUOUS_ATTR1",
            original_variable="continuous_attr1",
            readstat_type="double",
            description="",
            value_labels={99999: "missing"},
        )

        continuous2_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="CONTINUOUS_ATTR2",
            original_variable="continuous_attr2",
            readstat_type="double",
            description="",
            value_labels={99998: "missing", 99997: "also_missing"},
        )

        cat1_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="CAT_ATTR1",
            original_variable="cat_attr1",
            readstat_type="str",
            description="",
            value_labels={"a": "means_a", "b": "means_b", "c": "means_c", "missing": "missing"},
        )

        cat2_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="CAT_ATTR2",
            original_variable="cat_attr2",
            readstat_type="str",
            description="",
            value_labels={"x": "means_x", "y": "means_y", "missing": "missing"},
        )

        table = SavMetaTable(db_file=db_file)
        table.create_from_record(rinpersoon_meta)

        column_list = [
            rinpersoon_meta,
            rinpersoons_meta,
            irrelevant_meta,
            time1_meta,
            time2_meta,
            continuous1_meta,
            continuous2_meta,
            cat1_meta,
            cat2_meta,
        ]
        table.insert_many(column_list)

    @pytest.mark.usefixtures("metadata_db")
    @pytest.mark.usefixtures("bronze_data")
    def test_silver(self, db_file: Path, input_paths: dict[str, Path], tmp_path: Path):
        """Test conversion of bronze to silver."""
        dest_path = tmp_path / "silver.parquet"

        config = SilverConfig(
            rinpersoon_col="RINPERSOON",
            rinpersoons_col="RINPERSOONS",
            time_cols=["TIME1", "TIME2"],
            id_cols=["RINPERSOON", "IRRELEVANT_ID"],
            event_time_col="TIME1",
        )

        source_manifest = SourceManifest(db_file)

        # TODO: run this once as a fixture and then run the specific checks?
        convert_to_silver(
            source_manifest=source_manifest,
            source_path=input_paths["source_path"],
            source_filename=input_paths["source_filename"],
            dest_path=dest_path,
            config=config,
        )

        con = duckdb.connect()

        bronze_df = (
            pl.read_parquet(input_paths["bronze_file"])
            .filter(self.validity_mask == 1)
            .cast({"RINPERSOON": pl.Int64})
            .with_columns(pl.col("TIME1").str.to_date().alias(TIME_COLNAME))
            .sort(by=["RINPERSOON", TIME_COLNAME])
        )

        rel = con.read_parquet(dest_path)

        expected_columns = [PERSON_COLNAME, TIME_COLNAME, NOTE_COLNAME]
        assert rel.columns == expected_columns, "Incorrect columns"

        expected_types = ["bigint", "date", "struct"]
        col_types = [t.id for t in rel.types]
        assert col_types == expected_types, "Wrong column types"

        assert bronze_df.shape[0] == rel.shape[0], "Wrong number of rows"

        (
            pl.testing.assert_frame_equal(bronze_df.select(TIME_COLNAME), rel.select(TIME_COLNAME).pl()),
            "Rows out of order",
        )

        note_fields_and_types = rel.select(NOTE_COLNAME).types[0].children
        note_fields = [x[0] for x in note_fields_and_types]
        expected_fields = {"CONTINUOUS_ATTR1", "CONTINUOUS_ATTR2", "CAT_ATTR1", "CAT_ATTR2"}
        assert expected_fields == set(note_fields), "Note has incorrect fields."

        for col in ["CONTINUOUS_ATTR1", "CONTINUOUS_ATTR2"]:
            avg_null = (
                rel.select(f"CASE WHEN NOTE.{col} IS NULL THEN 1 ELSE 0 END AS X").aggregate("mean(X)").fetchall()[0][0]
            )
            assert avg_null > 0, "Missing values in continuous variables not coded as NULL."

        con.close()
