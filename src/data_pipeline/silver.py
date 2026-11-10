"""Build silver data.

For approx_count_distinct, see
- DuckDB implements https://arxiv.org/pdf/1702.01284, see
https://github.com/duckdb/duckdb/blob/2666f35707b758621f7b4e6e9694be35b41c10b5/src/include/duckdb/common/types/hyperloglog.hpp#L25
- Basic algorithm has ~= 2% error margin
    - https://en.wikipedia.org/wiki/HyperLogLog
    - https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
- As reported in the discussion here https://github.com/duckdb/duckdb/issues/20916,
the error varies depending on the registers used in the algorithm. For few registers (64), the
theoretical error is 13%. In some tests, the implementation had an average error
of 3%.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from enum import StrEnum
from itertools import filterfalse
from pathlib import Path
from typing import Self
import duckdb
from _duckdb import DuckDBPyRelation
from data_pipeline import silver_schema
from data_pipeline.duckdb_base import DUCKDB_NUMERIC_TYPES
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SilverMetaRecord
from data_pipeline.schemas import SourceManifest
from data_pipeline.silver_schema import NoteColumn
from data_pipeline.silver_schema import PrimaryKeyColumn
from data_pipeline.silver_schema import SecondaryKeyColumn

logger = logging.getLogger(__name__)


N_SIGMA = 6
APPROX_SD = 0.13

MAX_FRAC_DROP_DATE_CONVERSION = 0.2


class NoteAttributeType(StrEnum):
    """Encodes types of note attributes."""

    CONTINUOUS = "continuous"
    CATEGORICAL = "categorical"


@dataclass(frozen=True)
class Filter:
    """Mapping of silver entity column to constituting bronze columns.

    Parameters
    ----------
    column
        Column to filter.
    value
        Filter to apply.
    """

    column: str
    value: str

    @classmethod
    def from_dict(cls, input_dict: dict[str, str]) -> Self:
        """Instantiate from dictionary."""
        column, value = next(iter(input_dict.items()))
        return cls(column, value)


class SilverConfig:
    """Config for silver processing.

    Parameters
    ----------
    primary_key
        One-entry dictionary where key is the entity type from the entity
        vocabulary and the values is a string or a list of strings of
        column names in bronze that constitute the key.
        The bronze columns are assumed to be castable to 64-bit integer, or
        to ENUM for short categorical columns such as RINPERSOONS.
    filter_col
        Dictionary mapping bronze columns that should be filtered on having
        the corresponding value.
        A common use case is filtering on people in GBA, ie `RINPERSOONS="R"`.
        Currently only supports filtering on one column of string type, and
        filtering on one value.
        The filter column is dropped after filtering.
    time_cols
        List of column names in bronze that are timestamps.
    drop_cols
        List of column names in bronze to be dropped. A common use case
        are identifier columns that are not used in silver.
    secondary_keys
        Dictionary where keys are entity types from the entity vocabulary.
        Values are a string or a list of strings of constituting
        columns from bronze.
        The bronze columns are assumed to be castable to 64-bit integer, or
        to ENUM for short categorical columns such as RINPERSOONS.
        The order of the dictionary entries as well as the
        bronze column in the values determines the order
        in the resulting `RELATIONS` column.
    event_time_col
        Column name in bronze identifying the TIME.
    event_time_col_fmt
        Format of event_time_col in the bronze data (after converting to string).
        Follows the duckdb spec: https://duckdb.org/docs/lts/sql/functions/dateformat#format-specifiers
    apply_event_time
        If given, creates a new column referring to event time. Must match
        the format '%m-%d'. The currently only supported case
        is to define the month and day, and an event column is created from the
        year field in the metadata of the bronze field.


    Notes
    -----
    Columns are dropped in silver as follows:
        - drop_cols
        - time_cols that are not event_time_col.
        - the filter_col column.

    The config supports two uses of event time data:
        - using an existing column as the event time column. Requires specifying
        `event_time_col` and `event_time_col_fmt`.
        - applying a new column. Requires specifying `apply_event_time` an
        setting `event_time_col=None` and `time_cols=[]`. The latter assumes that
        the bronze file does not have any time column that needs to be dropped
        in silver.

    These constraints are currently not enforced by the config.

    The configuration is responsible for creating unique primary keys.
    For instance, this can be achieved by filtering on `RINPERSOONS`, dropping the column,
    and specifying only `RINPERSOON` as the primary key.
    Alternatively, the primary key can be defined as `["RINPERSOON", "RINPERSOONS"]`,
    in which case `RINPERSOONS` does not have to be dropped.

    See Also
    --------
    :py:data:`~data_pipeline.silver_schema.ENTITY_COLUMNS`
    :py:func:`~data_pipeline.silver.convert_to_silver`.

    """

    # TODO: refactor with the configuration file
    def __init__(  # noqa: PLR0913, PLR0917
        self,
        primary_key: dict[str, list[str] | str],
        secondary_keys: dict[str, list[str] | str] | None,
        filter_col: dict[str, str] | None,
        time_cols: list[str],
        event_time_col: str | None,
        event_time_col_fmt: str | None = "%Y-%m-%d",
        apply_event_time: str | None = None,
        drop_cols: list[str] | None = None,
    ):

        self.time_cols = time_cols
        self.event_time_col = event_time_col
        self.event_time_col_fmt = event_time_col_fmt
        self.apply_event_time = apply_event_time
        self.drop_cols = drop_cols
        # TODO for config validation: dictionaries can only have 1 entry!
        # TODO for config validation: in the dict, also the values need to be unique!
        self.filter_col = None if filter_col is None else Filter.from_dict(filter_col)
        self.primary_key = PrimaryKeyColumn.from_dict(primary_key)
        self.secondary_keys = SecondaryKeyColumn.from_dict(secondary_keys)

    @property
    def bronze_primary_key_columns(self) -> list[str]:
        """Name of the columns in the bronze file used for the primary key in silver."""
        return list(self.primary_key.entity.bronze_columns)

    @property
    def bronze_secondary_key_columns(self) -> list[str]:
        """Name of columns in the bronze file used for the secondary key in silver."""
        return self.secondary_keys.bronze_columns or []

    @property
    def silver_primary_key_column(self) -> str:
        """Name of the column in the bronze file used for the primary key in silver."""
        return self.primary_key.silver_column

    @property
    def cols_to_drop(self) -> list[str]:
        """Columns to drop from the table."""
        drop_cols = list(self.drop_cols or [])
        drop_cols += filterfalse(lambda x: x == self.event_time_col, self.time_cols)

        if self.filter_col:
            drop_cols += [self.filter_col.column]
        return drop_cols

    @property
    def event_idx_columns(self) -> list[Sequence[str] | str]:
        """List of columns defining an event."""
        if self.event_time_col:
            return [self.bronze_primary_key_columns, self.event_time_col]
        return [self.bronze_primary_key_columns, silver_schema.TIME_COLUMN]

    @property
    def has_event_col(self) -> bool:
        """Return True if event_time_col is defined."""
        return self.event_time_col is not None


class AttributeClassifier:
    """Classifier for event attributes.

    Parameters
    ----------
    rel
        Relation with the data to operate on.
    attr_cols
        List of column names in `rel` that are event attributes.

    Notes
    -----
    Columns are classified as categorical if the set of unique values
    in the column matches the set of categories in the .sav metadata.

    In practice, attributes are sequentially classified as:
        - continuous if their metadata has no value label
        - continuous if the approximate count of unique
        values is more than :py:data:`~data_pipeline.silver.N_SIGMA`
        standard deviations of the reference cardinality. The standard deviation
        is assumed to be :py:data:`~data_pipeline.silver.APPROX_SD` of the
        reference cardinality in the metadata, which is conservative.
        - continuous if the column is of numeric type and has all NaNs.
        - categorical if the exact count of unique values is less or equal the
        number of declared categories
        - continuous otherwise
    """

    def __init__(self, rel: DuckDBPyRelation, attr_col_meta: list[SavColumnMeta]):
        self.rel = rel
        self.attr_col_meta = attr_col_meta
        self.unassigned_cols: set[str] = {meta.variable for meta in attr_col_meta}
        self.cols_to_value_labels: dict[str, dict] = {}
        self.cols_to_attr_type: dict[str, str] = {}

    @property
    def is_done(self) -> bool:
        """If True, no unassigned columns are left."""
        return len(self.unassigned_cols) == 0

    def run(self) -> dict[str, str]:
        """Run the classification pipeline."""
        self.assign_on_missing_value_labels()
        if self.is_done:
            return self.cols_to_attr_type

        self.assign_on_approx_distinct()
        if self.is_done:
            return self.cols_to_attr_type

        self.assign_on_all_nans()
        if self.is_done:
            return self.cols_to_attr_type

        self.assign_on_exact_distinct()

        return self.cols_to_attr_type

    def assign_on_all_nans(self) -> None:
        """Assign continuous if either all rows are NaN or all rows are NULL.

        This is for variables that were not measured but retroactively added to the
        design schema.
        """
        candidate_cols = [col for col in self.unassigned_cols if self.rel.select(col).types[0] in DUCKDB_NUMERIC_TYPES]
        if len(candidate_cols) == 0:
            return

        nan_queries = [f"CASE WHEN isnan({x}) THEN 1 ELSE 0 END AS {x}" for x in candidate_cols]
        nan_query_sql = ",".join(nan_queries)

        null_queries = [f"CASE WHEN {x} IS NULL THEN 1 ELSE 0 END AS {x}" for x in candidate_cols]
        null_query_sql = ",".join(null_queries)

        aggregation_queries = ",".join([f"MEAN({x}) AS {x}" for x in candidate_cols])
        count_nans = self.rel.select(nan_query_sql).aggregate(aggregation_queries)
        count_nulls = self.rel.select(null_query_sql).aggregate(aggregation_queries)

        for col in candidate_cols:
            n_nan = count_nans.select(col).fetchall()[0][0]
            n_null = count_nulls.select(col).fetchall()[0][0]
            if n_nan < 1.0 and n_null < 1.0:
                continue
            self.cols_to_attr_type[col] = NoteAttributeType.CONTINUOUS
            self.unassigned_cols.remove(col)

    def assign_on_missing_value_labels(self) -> None:
        """Assign continuous the column has no value labels."""
        for col_meta in self.attr_col_meta:
            if col_meta.value_labels is None:
                self.cols_to_attr_type[col_meta.variable] = NoteAttributeType.CONTINUOUS
                self.unassigned_cols.remove(col_meta.variable)
                continue
            self.cols_to_value_labels[col_meta.variable] = col_meta.value_labels

    def assign_on_approx_distinct(self) -> None:
        """Assign continuous if the approximate cardinality is high enough."""
        col_queries = [f"approx_count_distinct({col}) AS {col}" for col in self.cols_to_value_labels]
        approx_distinct_rel = self.rel.aggregate(", ".join(col_queries)).execute()

        for col, value_labels in self.cols_to_value_labels.items():
            approx_distinct_count = approx_distinct_rel.select(col).fetchall()[0][0]

            reference_cardinality = len(value_labels)
            confidence_length = N_SIGMA * APPROX_SD * reference_cardinality
            if approx_distinct_count <= reference_cardinality + confidence_length:
                continue
            self.cols_to_attr_type[col] = NoteAttributeType.CONTINUOUS
            self.unassigned_cols.remove(col)

    def assign_on_exact_distinct(self) -> None:
        """Assign continuous if the exact cardinality count."""
        count_distincts = self.rel.aggregate(f"COUNT(DISTINCT COLUMNS({list(self.unassigned_cols)}))")

        for col in list(self.unassigned_cols):
            n_distinct = count_distincts.select(col).fetchall()[0][0]
            if n_distinct <= len(self.cols_to_value_labels[col]):
                self.cols_to_attr_type[col] = NoteAttributeType.CATEGORICAL
            else:
                self.cols_to_attr_type[col] = NoteAttributeType.CONTINUOUS
            self.unassigned_cols.remove(col)

    def assign_remaining(self) -> None:
        """Assign remaining columns."""
        for col in list(self.unassigned_cols):
            uniques = self.rel.select(col).distinct().fetchall()
            uniques = [x[0] for x in uniques]
            if set(uniques) == set(self.cols_to_value_labels[col].keys()):
                self.cols_to_attr_type[col] = NoteAttributeType.CATEGORICAL
                continue
            logger.info("Residual column %s classified as %s.", col, NoteAttributeType.CONTINUOUS)
            self.cols_to_attr_type[col] = NoteAttributeType.CONTINUOUS


def define_event_time_column(
    rel: DuckDBPyRelation, config: SilverConfig, file_record: FileMetaRecord
) -> DuckDBPyRelation:
    """Define the event-time column of data in a relation."""
    if config.has_event_col:
        rel_with_original_event_time = rel.select(config.event_time_col).limit(10)
        nrow_start = rel.shape[0]
        rel = rel.select(f"""* EXCLUDE({config.event_time_col}),
                         try_strptime({config.event_time_col}, '{config.event_time_col_fmt}')::DATE
                         AS {config.event_time_col}""")
        rel = rel.filter(f"{config.event_time_col} IS NOT NULL")
        nrow_end = rel.shape[0]
        if nrow_end <= (1 - MAX_FRAC_DROP_DATE_CONVERSION) * nrow_start:
            msg = (
                f"Date-casting reduced the number of rows by more "
                f"than {MAX_FRAC_DROP_DATE_CONVERSION * 100}%. Occurred in: {config.event_time_col} to DATE "
                f"with date format '{config.event_time_col_fmt}' \n"
                f"Here is a sample of what {config.event_time_col} looks like "
                f"in the bronze data:\n {rel_with_original_event_time.pl()}"
            )
            raise RuntimeError(msg)
    else:
        # TODO: task for config validation
        year = file_record.ref_period.year  # type: ignore[union-attr]
        year_col = f"{year}-{config.apply_event_time}"
        rel = rel.select(f"""*, '{year_col}'::DATE AS {silver_schema.TIME_COLUMN}""")

    return rel


def make_event_table(rel: DuckDBPyRelation, config: SilverConfig, attribute_cols: list[str]) -> DuckDBPyRelation:
    """Transform a relation into the event-table format.

    Parameters
    ----------
    rel
        Relation with the table to process.
    config
        The configuration class.
    attribute_cols
        Column names of event attributes.
    """
    note_column = NoteColumn(attribute_cols)
    note_query = note_column.event_table_query()

    relations_query = config.secondary_keys.event_table_query()
    primary_key_query = config.primary_key.event_table_query()

    time_colname_query = (
        f"{config.event_time_col} AS {silver_schema.TIME_COLUMN}" if config.has_event_col else silver_schema.TIME_COLUMN
    )
    rel = rel.select(f"{primary_key_query}, {time_colname_query}, {relations_query}, {note_query}")
    return rel.order(
        f"{config.silver_primary_key_column}, {silver_schema.TIME_COLUMN}, {silver_schema.RELATION_COLUMN}"
    )


def cleanse_continuous(rel: DuckDBPyRelation, attr_col_meta: list[SavColumnMeta]) -> DuckDBPyRelation:
    """Cast to FLOAT and replace NULLs as per metadata if required.

    Assumes that the column values as well as the declared value labels
    in the metadata are castable to float.

    Parameters
    ----------
    rel
        Relation with the data to operate on.
    attr_col_meta
        Metadata of columns to process.

    Raises
    ------
    ValueError
        If value labels of column metadata are not castable to float.
    duckdb.BinderException
        If the column is non-numeric but the column value label keys are float.
    """
    for col_meta in attr_col_meta:
        attr_name = col_meta.variable
        if col_meta.value_labels is None:
            logger.debug("No value labels declared for %s", attr_name)
            if rel.select(attr_name).types[0] not in DUCKDB_NUMERIC_TYPES:
                logger.debug("Casting %s to FLOAT", attr_name)
                rel = rel.select(f"* EXCLUDE {attr_name}, TRY_CAST({attr_name} AS FLOAT) AS {attr_name}")
            continue

        missing_values = [float(x) for x in col_meta.value_labels]

        # According to Kimi, the IN statement here fails if
        # missing_values is float but the column attr_name is non-numeric.
        rel = rel.select(f"""
           * EXCLUDE {attr_name},
            CASE WHEN
               {attr_name} IN {missing_values} THEN NULL::FLOAT
               ELSE {attr_name}::FLOAT
            END AS {attr_name}
        """)
    return rel


def convert_nans_to_null(rel: DuckDBPyRelation, attr_col_meta: list[SavColumnMeta]) -> DuckDBPyRelation:
    """Convert NaNs in continuous columns to NULL."""
    for col_meta in attr_col_meta:
        attr_name = col_meta.variable

        rel = rel.select(f"""
           * EXCLUDE {attr_name},
            CASE WHEN
               isnan({attr_name}) THEN NULL
               ELSE {attr_name}
            END AS {attr_name}
        """)

    return rel


def run_filter(rel: DuckDBPyRelation, config: SilverConfig) -> DuckDBPyRelation:
    """Filter columns in rel based on the configuration.

    Only works for VARCHAR columns.
    """
    if config.filter_col is None:
        return rel

    query = f"{config.filter_col.column} = '{config.filter_col.value}'"
    return rel.filter(query)


def cast_identifier_columns(
    rel: DuckDBPyRelation, config: SilverConfig, design_lookup: dict, sav_meta_table: SavMetaTable
) -> DuckDBPyRelation:
    """Cast all identifiers required for silver to BIGINT or ENUM."""
    columns_to_cast = config.bronze_primary_key_columns
    if config.bronze_secondary_key_columns is not None:
        columns_to_cast += config.bronze_secondary_key_columns

    key_cols_meta = [
        SavColumnMeta.from_table(lookup=design_lookup | {"variable": col}, table=sav_meta_table)
        for col in columns_to_cast
    ]
    cols_to_type = AttributeClassifier(rel, key_cols_meta).run()

    cast_query = []
    for column, type_ in cols_to_type.items():
        if type_ == NoteAttributeType.CONTINUOUS:
            cast_query += [f"CAST({column} AS BIGINT) AS {column}"]
        elif type_ == NoteAttributeType.CATEGORICAL:
            value_labels = rel.select(column).distinct().order(column).fetchall()
            value_labels_str: list[str] = [f"'{x[0]}'" for x in value_labels]
            cast_query += [f"CAST({column} as ENUM({','.join(value_labels_str)})) AS {column}"]  # type: ignore[arg-type]
    full_query = f"* EXCLUDE({','.join(cols_to_type.keys())}), {','.join(cast_query)}"
    return rel.select(full_query)


def convert_to_silver(
    source_manifest: SourceManifest, source_path: Path, source_filename: Path, dest_path: Path, config: SilverConfig
) -> SilverMetaRecord:
    """Convert bronze data to silver.

    The resulting parquet file has the following schema:

    .. list-table::
       :header-rows: 1

       * - Column
         - DuckDB type
         - Description
       * - ``ENTITY_ID``
         - ``BIGINT``
         - Unique entity identifier. Can be persons, companies or other. Needs to be one
         of the entities in :py:data:`~data_pipeline.silver_schema.ENTITY_COLUMNS`.
       * - ``DATE``
         - ``DATE``
         - Event date.
       * - ``RELATIONS``
         - ``STRUCT``
         - Key relating to other entities.
       * - ``NOTE``
         - ``STRUCT``
         - Event attributes; field names and types are inherited from the
           bronze columns (continuous fields are ``FLOAT``).

    For example, a row may look like
    ``{PERSON_ID: 12345, DATE: 2025-06-04, RELATIONS: {DWELLING_ID: 3}, NOTE: {attr1: 10, attr2: "Amsterdam"}}``.

    The mapping of columns in bronze to columns in silver is defined
    in the SilverConfig.
    No column for the event type is created; this should be specified
    in the location where the file is stored, using hive partitioning.

    The function processes data, using the column configuration from :py:class:`~data_pipeline.silver.SilverConfig`:
        1. Drops rows based on filter_col in the configuration
        2. Drops columns per the configuration
        3. All columns that are not dropped, used for timestamps, or primary/secondar
        keys, become NOTE attributes. The type of the attribute -- continuous or categorical --
        is inferred from the column's distribution and cardinality, compared to
        the one declared in the metadata.
        See :py:class:`~data_pipeline.silver.AttributeClassifier`.
        4. In continuous NOTE attributes, declared missing values are converted to NULL.

    The output table is ordered by:
        - primary key declared in the config
        - time column
        - keys of the secondary keys dictionary declared in the config

    Parameters
    ----------
    source_manifest
        Manifest with the source metadata.
    source_path
        Path to the original .sav file.
    source_filename
        Name of the original .sav file.
    dest_path
        Path where to save the silver file.
    config
        The silver processing configuration.

    Returns
    -------
    SilverMetaRecord
        Metadata record for the created silver file.

    Notes
    -----
    The function assumes that:
        - the columns declared as primary keys can be cast to
        BIGINT or ENUM without error.
        - the file metadata have a non-NULL `ref_period` if the config defines
        a new event time column.
    """
    design_lookup = {"source_path": source_path, "source_filename": source_filename}
    file_record = FileMetaRecord.from_table(design_lookup, table=source_manifest)
    sav_meta_table = SavMetaTable(db_file=source_manifest.db_file)

    con = duckdb.connect()

    rel = con.read_parquet(file_record.bronze_path)  # type: ignore[arg-type]
    rel = run_filter(rel, config)
    rel = define_event_time_column(rel, config, file_record)
    rel = cast_identifier_columns(rel, config, design_lookup, sav_meta_table)

    if len(config.cols_to_drop) > 0:
        rel = rel.select(f"* EXCLUDE ({','.join(config.cols_to_drop)})")

    columns = rel.columns
    attribute_columns = filterfalse(
        lambda col: (
            col in config.event_idx_columns
            or col in config.bronze_primary_key_columns
            or col in config.bronze_secondary_key_columns
        ),
        columns,
    )
    attribute_columns_meta = [
        SavColumnMeta.from_table(lookup=design_lookup | {"variable": col}, table=sav_meta_table)
        for col in attribute_columns
    ]

    # Use the full table to avoid any accidental misclassification
    full_table_rel = con.read_parquet(file_record.bronze_path)  # type: ignore[arg-type]
    classifier = AttributeClassifier(full_table_rel, attribute_columns_meta)
    cols_to_attr_type = classifier.run()

    continuous_col_meta = [
        m for m in attribute_columns_meta if cols_to_attr_type[m.variable] == NoteAttributeType.CONTINUOUS
    ]
    rel = cleanse_continuous(rel, continuous_col_meta)
    rel = convert_nans_to_null(rel, continuous_col_meta)
    rel = make_event_table(rel, config, list(cols_to_attr_type.keys()))

    rel.to_parquet(str(dest_path))
    con.close()
    # NOTE: see docs for potential speedups: https://duckdb.org/docs/lts/clients/python/relational_api#write_parquet
    # for instance, the `per_thread_output` option
    # Also consider adding a memory limit to the connection if necessary

    # TODO: config validation should take care of the typing errors here
    return SilverMetaRecord(
        silver_path=dest_path.parent,
        file_name=Path(dest_path.name),
        bronze_path=Path(file_record.bronze_path),  # type: ignore[arg-type]
        bronze_primary_key_cols=config.bronze_primary_key_columns,
        bronze_secondary_key_cols=config.secondary_keys.to_dict(),
        bronze_event_time_col=config.event_time_col if config.has_event_col else config.apply_event_time,  # type: ignore[arg-type]
        bronze_cols_dropped=config.cols_to_drop,
        last_modified=datetime.now(UTC),
        event_note_types=cols_to_attr_type,
    )


# Todo
# create quarantine table?
