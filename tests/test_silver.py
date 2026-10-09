from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
import numpy as np
import polars as pl
import pytest
from _duckdb import DuckDBPyConnection
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest
from data_pipeline.silver import NOTE_COLNAME
from data_pipeline.silver import PERSON_COLNAME
from data_pipeline.silver import TIME_COLNAME
from data_pipeline.silver import AttributeClassifier
from data_pipeline.silver import NoteAttributeType
from data_pipeline.silver import SilverConfig
from data_pipeline.silver import convert_to_silver
from .conftest import TestData


@pytest.fixture
def db_con() -> Generator[duckdb.DuckDBPyConnection]:
    """Open temporary duckdb connection for duration of the test."""
    con = duckdb.connect()
    yield con
    con.close()


class TestClassifyAttributes(TestData):
    """Test the attribute classification."""

    def generate_input_data(
        self, con: DuckDBPyConnection, input_dict: dict[str, tuple[str, list[str] | None, float | None]]
    ) -> tuple[duckdb.DuckDBPyRelation, list[SavColumnMeta]]:
        """Generate input data along metadata."""
        data_dict = {}
        metadata = []
        for col, (attribute_type, value_label_keys, user_frac_to_corrupt) in input_dict.items():
            if attribute_type == NoteAttributeType.CATEGORICAL:
                if value_label_keys is None:
                    msg = "For categorical columns, value_label_keys are necessary"
                    raise RuntimeError(msg)
                data_dict[col] = self.make_categorical(value_label_keys)
            elif attribute_type == NoteAttributeType.CONTINUOUS:
                col_array = self.rng.random(size=self.sample_size).astype(np.float64)
                if value_label_keys:
                    frac_to_corrupt = 0.05 if user_frac_to_corrupt is None else user_frac_to_corrupt
                    col_array = self.corrupt(
                        col_array, inject=value_label_keys, size=int(frac_to_corrupt * self.sample_size)
                    )

                data_dict[col] = col_array

            current_labels = None
            if value_label_keys:
                current_labels = dict.fromkeys(value_label_keys, "")  # label value unused in this function

            meta = SavColumnMeta(
                variable=col,
                value_labels=current_labels,
                # -- same for all
                source_path=Path("some/path"),
                source_filename=Path("file.sav"),
                original_variable="some_var",
                readstat_type="not_a_real_type",
                description="",
            )

            metadata.append(meta)

        rel = con.from_arrow(pl.DataFrame(data_dict).to_arrow())

        return rel, metadata

    @pytest.mark.parametrize(
        ("input_dict"),
        [
            {
                "col_a": (NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"], None),
                "col_b": (NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"], None),
            },
            {
                "col_a": (NoteAttributeType.CONTINUOUS, None, None),
                "col_b": (NoteAttributeType.CONTINUOUS, None, None),
            },
            {
                "col_a": (NoteAttributeType.CONTINUOUS, None, None),
                "col_b": (NoteAttributeType.CONTINUOUS, [99999.0, 99998.0], None),
            },
            {
                "col_a": (NoteAttributeType.CONTINUOUS, None, None),
                "col_b": (NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"], None),
                "col_c": (NoteAttributeType.CATEGORICAL, list(range(100)), None),
            },
            {
                "col_a": (NoteAttributeType.CONTINUOUS, None, None),
                "col_b": (NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"], None),
                "col_c": (NoteAttributeType.CATEGORICAL, list(range(1_000)), None),
            },
            {
                "col_a": (NoteAttributeType.CONTINUOUS, None, None),
                "col_b": (NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"], None),
                "col_c": (NoteAttributeType.CATEGORICAL, list(range(10_000)), None),
            },
            {
                "col_a": (NoteAttributeType.CONTINUOUS, None, None),
                "col_b": (NoteAttributeType.CONTINUOUS, list(range(10_000)), 0.5),
            },
        ],
        ids=[
            "categorical_only",
            "continuous_only",
            "continuous_only_with_value_labels",
            "100_categories",
            "1k_categories",
            "10k_categories",
            "continuous_with_many_value_labels",
        ],
    )
    def test_classify_attributes(
        self, db_con: DuckDBPyConnection, input_dict: dict[str, tuple[str, list[str] | None, float | None]]
    ):
        """Test the classify_attributes function."""
        rel, attr_col_meta = self.generate_input_data(db_con, input_dict)
        classifier = AttributeClassifier(rel, attr_col_meta)
        result = classifier.run()
        for col, (expected_type, _, _) in input_dict.items():
            assert result[col] == expected_type, f"event attribute {col} incorrectly classified"


config_with_existing_time_col = SilverConfig(
    rinpersoon_col="RINPERSOON",
    rinpersoons_col="RINPERSOONS",
    time_cols=["TIME1", "TIME2"],
    id_cols=["RINPERSOON", "IRRELEVANT_ID"],
    event_time_col="TIME1",
)
config_without_existing_time_col = SilverConfig(
    rinpersoon_col="RINPERSOON",
    rinpersoons_col="RINPERSOONS",
    time_cols=[],  # TODO: work also with declard time col (but event_time_col = None?)
    id_cols=["RINPERSOON", "IRRELEVANT_ID"],
    event_time_col=None,
    event_time_col_fmt=None,
    apply_event_time="01-01",
)


@pytest.mark.parametrize(
    "config",
    [(config_with_existing_time_col), (config_without_existing_time_col)],
    ids=["use_existing_time_col", "make_new_time_col"],
)
class TestSilver(TestData):
    """Class for testing bronze->silver conversion."""

    bronze_ref_year: int = 2024

    @pytest.fixture
    def input_paths(self, tmp_path: Path) -> dict[str, Path]:
        """Create dictionary with paths, used as inputs for tests."""
        return {
            "source_path": Path("/path/to/raw/data"),
            "source_filename": Path("raw_file.sav"),
            "bronze_file": tmp_path / "bronze.parquet",
        }

    def create_bronze_data(self, input_paths: dict[str, Path], config: SilverConfig) -> None:
        """Bronze data used is input for silver.

        Arguments
        ---------
        input_paths:
            Input paths for bronze data; normally the `input_paths` fixture.
        config:
            Silver config with information whether the user defines a event time
            column or whether it's taken from the bronze data.
        """
        self.validity_mask = np.bool(np.ones(self.sample_size))

        """Create .parquet of bronze."""
        data_dict = {
            "RINPERSOON": self.make_identifiers(),
            "RINPERSOONS": self.make_categorical(["R"], [1]),
            "IRRELEVANT_ID": self.make_identifiers(),
            "CONTINUOUS_ATTR1": self.make_continuous(),
            "CONTINUOUS_ATTR2": self.make_continuous(),
            "CAT_ATTR1": self.make_categorical(["a", "b", "c", "missing"], [0.1, 0.6, 0.2, 0.1]),
            "CAT_ATTR2": self.make_categorical(["x", "y", "missing"], [0.2, 0.75, 0.05]),
            "CONTINUOUS_ATTR3_NO_MISSING": self.make_continuous(),
        }

        data_dict["CONTINUOUS_ATTR1"] = self.corrupt(
            x=data_dict["CONTINUOUS_ATTR1"], inject=[99999], size=int(0.02 * self.sample_size)
        )

        data_dict["CONTINUOUS_ATTR2"] = self.corrupt(
            x=data_dict["CONTINUOUS_ATTR2"],
            inject=[99998.0, 99997.0],
            probs=[1 / 3, 2 / 3],
            size=int(0.03 * self.sample_size),
        )

        data_dict["RINPERSOONS"] = self.corrupt(
            x=data_dict["RINPERSOONS"], inject=["not_R"], size=int(0.05 * self.sample_size), valid=False
        )

        if config.event_time_col:
            data_dict |= {
                "TIME1": self.random_dates().astype(str),
                "TIME2": self.random_dates().astype(str),
            }

            data_dict["TIME1"] = self.corrupt(
                x=data_dict["TIME1"], inject=["--------"], size=int(0.02 * self.sample_size), valid=False
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
            ref_period=datetime(self.bronze_ref_year, 1, 1, tzinfo=UTC),
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

        continuous3_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="CONTINUOUS_ATTR3_NO_MISSING",
            original_variable="continuous_attr3",
            readstat_type="double",
            description="",
            value_labels=None,
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
            continuous3_meta,
        ]
        table.insert_many(column_list)

    @pytest.mark.usefixtures("metadata_db")
    def test_silver(self, db_file: Path, input_paths: dict[str, Path], tmp_path: Path, config: SilverConfig):
        """Test conversion of bronze to silver."""
        self.create_bronze_data(input_paths, config)
        dest_path = tmp_path / "silver.parquet"

        source_manifest = SourceManifest(db_file)

        _ = convert_to_silver(
            source_manifest=source_manifest,
            source_path=input_paths["source_path"],
            source_filename=input_paths["source_filename"],
            dest_path=dest_path,
            config=config,
        )

        con = duckdb.connect()

        silver_df = (
            pl.read_parquet(input_paths["bronze_file"]).filter(self.validity_mask == 1).cast({"RINPERSOON": pl.Int64})
        )

        if config.event_time_col:
            silver_df = silver_df.with_columns(pl.col("TIME1").str.to_date().alias(TIME_COLNAME))
        else:
            year_col = f"{self.bronze_ref_year}-{config.apply_event_time}"
            silver_df = silver_df.with_columns(pl.lit(year_col).str.to_date().alias(TIME_COLNAME))

        silver_df = silver_df.sort(by=["RINPERSOON", TIME_COLNAME])

        rel = con.read_parquet(dest_path)

        expected_columns = [PERSON_COLNAME, TIME_COLNAME, NOTE_COLNAME]
        assert rel.columns == expected_columns, "Incorrect columns"

        expected_types = ["bigint", "date", "struct"]
        col_types = [t.id for t in rel.types]
        assert col_types == expected_types, "Wrong column types"

        assert silver_df.shape[0] == rel.shape[0], "Wrong number of rows"

        (
            pl.testing.assert_frame_equal(silver_df.select(TIME_COLNAME), rel.select(TIME_COLNAME).pl()),
            "Rows out of order",
        )

        note_fields_and_types = rel.select(NOTE_COLNAME).types[0].children
        note_fields = [x[0] for x in note_fields_and_types]
        expected_fields = {
            "CONTINUOUS_ATTR1",
            "CONTINUOUS_ATTR2",
            "CONTINUOUS_ATTR3_NO_MISSING",
            "CAT_ATTR1",
            "CAT_ATTR2",
        }
        assert expected_fields == set(note_fields), "Note has incorrect fields."

        for col in ["CONTINUOUS_ATTR1", "CONTINUOUS_ATTR2"]:
            avg_null = (
                rel.select(f"CASE WHEN NOTE.{col} IS NULL THEN 1 ELSE 0 END AS X").aggregate("mean(X)").fetchall()[0][0]
            )
            assert avg_null > 0, "Missing values in continuous variables not coded as NULL."

        for col in ["CONTINUOUS_ATTR3_NO_MISSING"]:
            avg_null = (
                rel.select(f"CASE WHEN NOTE.{col} IS NULL THEN 1 ELSE 0 END AS X").aggregate("mean(X)").fetchall()[0][0]
            )
            assert avg_null == 0, "Missing values added when they should not be."

        con.close()
