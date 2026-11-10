from collections.abc import Callable
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
import duckdb
import duckdb.sqltypes
import numpy as np
import polars as pl
import pytest
from _duckdb import DuckDBPyConnection
from data_pipeline import silver_schema
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest
from data_pipeline.silver import AttributeClassifier
from data_pipeline.silver import NoteAttributeType
from data_pipeline.silver import SilverConfig
from data_pipeline.silver import convert_to_silver
from data_pipeline.silver import make_event_table
from .conftest import TestData


def get_struct_fields_and_types(
    rel: duckdb.DuckDBPyRelation, column: str
) -> tuple[list[str], list[duckdb.sqltypes.DuckDBPyType | int | list[str]]]:
    """Get the fields and types from a struct column in a DuckDBPyRelation."""
    fields_and_types = rel.select(column).types[0].children
    fields = [x[0] for x in fields_and_types]
    types = [x[1] for x in fields_and_types]
    return fields, types


@pytest.fixture
def db_con() -> Generator[duckdb.DuckDBPyConnection]:
    """Open temporary duckdb connection for duration of the test."""
    con = duckdb.connect()
    yield con
    con.close()


@dataclass
class ColumnConfig:
    """Configuration for creating one column of test data.

    Closely ties into conftest.TestData.

    Arguments
    ---------
    attribute_type: The note attribute type to generate.
    value_label_keys:
        The value label keys to for the .sav metadata.
        For continuous variables,
        this is used to inject missing values in the .sav meaning.
        For categorical variables, it is the modalities to draw from.
    frac_to_corrupt:
        For continuous variables: the fraction of rows to corrupt.
        Ignored for categorical variables.
    generation_func:
        Function to generate the data.
        Needs to be None for categorical variables as a default is used.
        For continuous, needs to be a function using `TestData.rng`. If not given,
        rng.random is used.
    expected_attr_type:
        Expected note attribute type. Defaults to the `attribute_type`.
    """

    attribute_type: NoteAttributeType
    value_label_keys: list[str | float | int | None] | None
    frac_to_corrupt: float | None = None
    generation_func: Callable | None = None
    expected_attr_type: NoteAttributeType | None = None

    def __post_init__(self):
        if self.attribute_type == NoteAttributeType.CATEGORICAL and self.generation_func is not None:
            msg = "Custom generation functions are only supported for continuous columns."
            raise NotImplementedError(msg)

        if self.attribute_type == NoteAttributeType.CONTINUOUS and self.generation_func is None:
            self.generation_func = lambda td: td.rng.random(td.sample_size).astype(np.float64)

        self.expected_attr_type = self.expected_attr_type or self.attribute_type


class TestClassifyAttributes(TestData):
    """Test the attribute classification."""

    def generate_input_data(
        self, con: DuckDBPyConnection, input_dict: dict[str, ColumnConfig]
    ) -> tuple[duckdb.DuckDBPyRelation, list[SavColumnMeta]]:
        """Generate input data along metadata."""
        data_dict = {}
        metadata = []
        for col, config in input_dict.items():
            if config.attribute_type == NoteAttributeType.CATEGORICAL:
                if config.value_label_keys is None:
                    msg = "For categorical columns, value_label_keys are necessary"
                    raise RuntimeError(msg)
                data_dict[col] = self.make_categorical(config.value_label_keys)

            elif config.attribute_type == NoteAttributeType.CONTINUOUS:
                assert config.generation_func is not None
                col_array = config.generation_func(self)
                if config.value_label_keys:
                    frac_to_corrupt = 0.05 if config.frac_to_corrupt is None else config.frac_to_corrupt
                    col_array = self.corrupt(
                        col_array, inject=config.value_label_keys, size=int(frac_to_corrupt * self.sample_size)
                    )

                data_dict[col] = col_array

            current_labels = None
            if config.value_label_keys:
                current_labels = dict.fromkeys(config.value_label_keys, "")  # label value unused in this function

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
                "col_a": ColumnConfig(NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"]),
                "col_b": ColumnConfig(NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"]),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CONTINUOUS, [99999.0, 99998.0]),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"]),
                "col_c": ColumnConfig(NoteAttributeType.CATEGORICAL, list(range(100))),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"]),
                "col_c": ColumnConfig(NoteAttributeType.CATEGORICAL, list(range(1_000))),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CATEGORICAL, ["class_a", "class_b", "missing"]),
                "col_c": ColumnConfig(NoteAttributeType.CATEGORICAL, list(range(10_000))),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CONTINUOUS, list(range(10_000)), 0.5),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(NoteAttributeType.CONTINUOUS, [None], 1),
            },
            {
                "col_a": ColumnConfig(NoteAttributeType.CONTINUOUS, None),
                "col_b": ColumnConfig(
                    NoteAttributeType.CONTINUOUS,
                    [777, 888, 999],
                    frac_to_corrupt=0,
                    generation_func=lambda td: td.rng.binomial(n=1, p=0.5, size=td.sample_size),
                    expected_attr_type=NoteAttributeType.CATEGORICAL,
                ),
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
            "continuous_all_missing",
            "continuous_col_with_few_modalities",
        ],
    )
    def test_classify_attributes(self, db_con: DuckDBPyConnection, input_dict: dict[str, ColumnConfig]):
        """Test the classify_attributes function."""
        rel, attr_col_meta = self.generate_input_data(db_con, input_dict)
        classifier = AttributeClassifier(rel, attr_col_meta)
        result = classifier.run()
        for col, config in input_dict.items():
            expected_type = config.expected_attr_type
            assert result[col] == expected_type, f"event attribute {col} incorrectly classified"


config_with_existing_time_col = SilverConfig(
    primary_key={"person": "RINPERSOON"},
    filter_col={"RINPERSOONS": "R"},
    time_cols=["TIME1", "TIME2"],
    drop_cols=["IRRELEVANT_ID"],
    event_time_col="TIME1",
)
config_without_existing_time_col = SilverConfig(
    primary_key={"person": "RINPERSOON"},
    filter_col={"RINPERSOONS": "R"},
    drop_cols=["IRRELEVANT_ID"],
    apply_event_time="01-01",
)
config_with_composite_primary_key = SilverConfig(
    primary_key={"person": ["RINPERSOON", "RINPERSOONS"]},
    time_cols=["TIME1", "TIME2"],
    drop_cols=["IRRELEVANT_ID"],
    event_time_col="TIME1",
)
# the following writes IRRELEVANT_ID and TIME2 to NOTE; just for testing
# TODO: this test should be refactored eventually
config_without_dropping_cols = SilverConfig(
    primary_key={"person": ["RINPERSOON", "RINPERSOONS"]},
    time_cols=["TIME1"],
    event_time_col="TIME1",
)


@pytest.mark.parametrize(
    ("config", "expected_primary_key_type"),
    [
        (config_with_existing_time_col, "bigint"),
        (config_without_existing_time_col, "bigint"),
        (config_with_composite_primary_key, "struct"),
        (config_without_dropping_cols, "struct"),
    ],
    ids=["use_existing_time_col", "make_new_time_col", "primary_key_with_enum", "no_columns_to_drop"],
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
            # for instance: "financial independence"
            "ORDINAL_ATTR_AS_STR": self.make_categorical(list(range(10))).astype(str),
            "ORDINAL_ATTR_AS_STR_W_EMPTY": self.make_categorical(["1", "2", ""]).astype(str),
            "NULL_COL": self.make_null_col(),
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

        rinpersoons_valid = "RINPERSOONS" in config.bronze_primary_key_columns
        data_dict["RINPERSOONS"] = self.corrupt(
            x=data_dict["RINPERSOONS"], inject=["not_R"], size=int(0.05 * self.sample_size), valid=rinpersoons_valid
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

        ordinal_attr_as_str_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="ORDINAL_ATTR_AS_STR",
            original_variable="continuous_attr4",
            readstat_type="str",
            description="",
            value_labels=None,
        )

        ordinal_attr_as_str_w_empty_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="ORDINAL_ATTR_AS_STR_W_EMPTY",
            original_variable="continuous_attr5",
            readstat_type="str",
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

        null_col_meta = SavColumnMeta(
            source_path=source_path,
            source_filename=source_filename,
            variable="NULL_COL",
            original_variable="null_col",
            readstat_type="str",
            description="",
            value_labels=None,
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
            ordinal_attr_as_str_meta,
            ordinal_attr_as_str_w_empty_meta,
            null_col_meta,
        ]
        table.insert_many(column_list)

    @pytest.mark.usefixtures("metadata_db")
    def test_convert_to_silver(  # noqa: PLR0913, PLR0917
        self,
        db_con: DuckDBPyConnection,
        db_file: Path,
        input_paths: dict[str, Path],
        tmp_path: Path,
        config: SilverConfig,
        expected_primary_key_type: str,
    ):
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

        silver_df = (
            pl.read_parquet(input_paths["bronze_file"]).filter(self.validity_mask == 1).cast({"RINPERSOON": pl.Int64})
        )

        if config.event_time_col:
            silver_df = silver_df.with_columns(pl.col("TIME1").str.to_date().alias(silver_schema.TIME_COLUMN))
        else:
            year_col = f"{self.bronze_ref_year}-{config.apply_event_time}"
            silver_df = silver_df.with_columns(pl.lit(year_col).str.to_date().alias(silver_schema.TIME_COLUMN))

        sort_keys = [*config.bronze_primary_key_columns, silver_schema.TIME_COLUMN]
        silver_df = silver_df.sort(by=sort_keys)

        rel = db_con.read_parquet(dest_path)

        expected_columns = [
            silver_schema.ENTITY_COLUMNS["person"],
            silver_schema.TIME_COLUMN,
            silver_schema.RELATION_COLUMN,
            silver_schema.NOTE_COLUMN,
        ]
        assert rel.columns == expected_columns, "Incorrect columns"

        expected_types = [expected_primary_key_type, "date", "integer", "struct"]
        col_types = [t.id for t in rel.types]
        assert col_types == expected_types, "Wrong column types"

        assert silver_df.shape[0] == rel.shape[0], "Wrong number of rows"

        pl.testing.assert_frame_equal(
            silver_df.select(silver_schema.TIME_COLUMN), rel.select(silver_schema.TIME_COLUMN).pl()
        )

        fields, _ = get_struct_fields_and_types(rel, silver_schema.NOTE_COLUMN)
        expected_fields = {
            "CONTINUOUS_ATTR1",
            "CONTINUOUS_ATTR2",
            "CONTINUOUS_ATTR3_NO_MISSING",
            "ORDINAL_ATTR_AS_STR",
            "ORDINAL_ATTR_AS_STR_W_EMPTY",
            "NULL_COL",
            "CAT_ATTR1",
            "CAT_ATTR2",
        }
        if len(config.cols_to_drop) == 0:
            expected_fields.update(["IRRELEVANT_ID", "TIME2"])

        assert expected_fields == set(fields), "Note has incorrect fields."

        continuous_cols_missing_frac = {
            "CONTINUOUS_ATTR1": lambda x: x > 0,
            "CONTINUOUS_ATTR2": lambda x: x > 0,
            "CONTINUOUS_ATTR3_NO_MISSING": lambda x: x == 0,
            "ORDINAL_ATTR_AS_STR_W_EMPTY": lambda x: x > 0,
            "ORDINAL_ATTR_AS_STR": lambda x: x == 0,
            "NULL_COL": lambda x: x == 1,
        }

        for col, comparison_func in continuous_cols_missing_frac.items():
            avg_null = (
                rel.select(f"CASE WHEN NOTE.{col} IS NULL THEN 1 ELSE 0 END AS X").aggregate("mean(X)").fetchall()[0][0]
            )
            assert comparison_func(avg_null), f"Missing values incorrectly coded for {col}"


class TestMakeEventTable(TestData):
    """Class for testing making the event table."""

    @pytest.mark.parametrize(
        ("secondary_key_config", "expected_col_type_secondary_key"),
        [
            ({"school": "SCHOOLID", "dwelling": "RINOBJECTNR"}, "struct"),
            (None, "integer"),
        ],
        ids=["2_relations", "no_relations"],
    )
    @pytest.mark.parametrize(
        ("has_event_attrs", "expected_col_type_note"),
        [(True, "struct"), (False, "integer")],
        ids=["with_note_attrs", "no_note_attrs"],
    )
    def test_make_event_table(
        self,
        db_con: DuckDBPyConnection,
        secondary_key_config: dict[str, list[str] | str],
        expected_col_type_secondary_key: str,
        has_event_attrs: bool,
        expected_col_type_note: str,
    ):
        """Test make_event_table function."""
        data_dict = {
            "PERSON": self.make_identifiers().astype(np.int64),
            "SCHOOLID": self.make_identifiers(500).astype(np.int64),
            "RINOBJECTNR": self.make_identifiers(300_000).astype(np.int64),
            "TIME1": self.random_dates(),
        }
        attribute_cols = []

        if has_event_attrs:
            attribute_dict = {
                "ATTR1": self.make_continuous(),
                "ATTR2": self.make_categorical(["a", "b", "c", "missing"], [0.1, 0.6, 0.2, 0.1]),
            }
            data_dict |= attribute_dict
            attribute_cols = list(attribute_dict.keys())

        config = SilverConfig(
            primary_key={"person": "PERSON"},
            time_cols=["TIME1"],
            secondary_keys=secondary_key_config,
            event_time_col="TIME1",
        )

        rel = db_con.from_arrow(pl.DataFrame(data_dict).to_arrow())
        result = make_event_table(rel, config, attribute_cols)
        expected_columns = [
            silver_schema.ENTITY_COLUMNS["person"],
            silver_schema.TIME_COLUMN,
            silver_schema.RELATION_COLUMN,
            silver_schema.NOTE_COLUMN,
        ]
        assert result.columns == expected_columns, "Incorrect columns"
        expected_types = ["bigint", "date", expected_col_type_secondary_key, expected_col_type_note]
        col_types = [t.id for t in result.types]
        assert col_types == expected_types, "Wrong column types"

        ordered_result = result.order(
            f"{silver_schema.ENTITY_COLUMNS['person']}, {silver_schema.TIME_COLUMN}, {silver_schema.RELATION_COLUMN}"
        )
        pl.testing.assert_frame_equal(ordered_result.pl(), result.pl())

        if expected_col_type_note == "struct":
            fields, types = get_struct_fields_and_types(result, silver_schema.NOTE_COLUMN)
            assert set(fields) == set(attribute_cols), "Wrong attribute columns"

        if expected_col_type_secondary_key == "struct":
            fields, types = get_struct_fields_and_types(result, silver_schema.RELATION_COLUMN)

            assert fields == [silver_schema.ENTITY_COLUMNS["school"], silver_schema.ENTITY_COLUMNS["dwelling"]], (
                f"wrong fields for {silver_schema.RELATION_COLUMN} column"
            )
            assert types == [duckdb.sqltypes.DuckDBPyType(int), duckdb.sqltypes.DuckDBPyType(int)], (
                f"wrong types for {silver_schema.RELATION_COLUMN} column"
            )
        else:
            rows_with_null_relations = result.filter(f"{silver_schema.RELATION_COLUMN} IS NULL")
            assert rows_with_null_relations.shape[0] == rel.shape[0], (
                f"Not all rows of {silver_schema.RELATION_COLUMN} are NULL"
            )
