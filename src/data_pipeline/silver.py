import logging
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from enum import StrEnum
from itertools import filterfalse
from pathlib import Path
import duckdb
from _duckdb import DuckDBPyRelation
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SilverMetaRecord
from data_pipeline.schemas import SourceManifest

logger = logging.getLogger(__name__)

TIME_COLNAME = "DATE"
PERSON_COLNAME = "RINPERSOON"
NOTE_COLNAME = "NOTE"


class NoteAttributeType(StrEnum):
    """Encodes types of note attributes."""

    continuous = "continuous"
    categorical = "categorical"


@dataclass
class SilverConfig:
    """Config for silver processing.

    Arguments
    ---------
    rinpersoon_col:
        Column name in bronze identifying the RINPERSOON.
    rinpersoons_col:
        Column name in bronze identifying people in GBA (normally the
        `RINPERSOONS` column).
    time_cols:
        List of column names in bronze that are timestamps.
    id_cols:
        List of column names in bronze that are identifiers.
    event_time_col:
        Column name in bronze identifying the TIME.
    event_time_col_fmt:
        Format of event_time_col in the bronze data (after converting to string).
        Follows the duckdb spec: https://duckdb.org/docs/lts/sql/functions/dateformat#format-specifiers
    apply_event_time:
        If given, creates a new column referring to event time. Must match
        the format '%m-%d'. The currently only supported case
        is to define the month and day, and an event column is created from the
        year field in the metadata of the bronze field.

    Notes
    -----
    Columns are dropped in silver as follows:
        - id_cols that are not rinpersoon_col.
        - time_cols that are not event_time_col.
        - the RINPERSOONS column

    The config supports two uses of event time data:
        - using an existing column as the event time column. Requires specifying
        `event_time_col` and `event_time_col_fmt`.
        - applying a new column. Requires specifying `apply_event_time` and
        setting `event_time_col=None` and `time_cols=[]`. The latter assumes that
        the bronze file does not have any time column that needs to be dropped
        in silver.

    These constraints are currently not enforced by the config.
    """

    rinpersoon_col: str
    rinpersoons_col: str
    time_cols: list[str]
    id_cols: list[str]
    event_time_col: str | None
    event_time_col_fmt: str | None = "%Y-%m-%d"
    apply_event_time: str | None = None

    @property
    def cols_to_drop(self) -> list[str]:
        """Columns to drop from the table."""
        drop_cols: list[str] = []
        drop_cols += filterfalse(lambda x: x == self.event_time_col, self.time_cols)
        drop_cols += filterfalse(lambda x: x == self.rinpersoon_col, self.id_cols)
        drop_cols += [self.rinpersoons_col]
        return drop_cols

    @property
    def event_idx_columns(self) -> list[str | None]:
        """List of columns defining an event."""
        if self.event_time_col:
            return [self.rinpersoon_col, self.event_time_col]
        return [self.rinpersoon_col, TIME_COLNAME]

    @property
    def has_event_col(self) -> bool:
        """Return True if event_time_col is defined."""
        return self.event_time_col is not None


def define_event_time_col(rel: DuckDBPyRelation, config: SilverConfig, file_record: FileMetaRecord) -> DuckDBPyRelation:
    """Define the event-time column of data in a relation."""
    if config.has_event_col:
        rel = rel.select(f"""* EXCLUDE({config.event_time_col}),
                         try_strptime({config.event_time_col}, '{config.event_time_col_fmt}')::DATE
                         AS {config.event_time_col}""")
        rel = rel.filter(f"{config.event_time_col} IS NOT NULL")
    else:
        # TODO: task for config validation
        year = file_record.ref_period.year  # type: ignore[union-attr]
        year_col = f"{year}-{config.apply_event_time}"
        rel = rel.select(f"""*, '{year_col}'::DATE AS {TIME_COLNAME}""")

    return rel


def make_event_table(rel: DuckDBPyRelation, config: SilverConfig, attribute_cols: list[str]) -> DuckDBPyRelation:
    """Transform a relation into the event-table format.

    Arguments
    ---------
    rel:
        Relation with the table to process.
    config:
        The configuration class.
    attribute_cols:
        Column names of event attributes.
    """
    struct_query_inputs = [f"{col} := {col}" for col in attribute_cols]
    struct_query = ", ".join(struct_query_inputs)
    time_colname_query = f"{config.event_time_col} AS {TIME_COLNAME}" if config.has_event_col else TIME_COLNAME
    rel = rel.select(f"""{config.rinpersoon_col} AS {PERSON_COLNAME},
                     {time_colname_query},
                     struct_pack({struct_query}) AS {NOTE_COLNAME}
                     """)
    return rel.order(f"{PERSON_COLNAME}, {TIME_COLNAME}")


def classify_attributes(rel: DuckDBPyRelation, attr_col_meta: list[SavColumnMeta]) -> dict[str, str]:
    """Classify attributes as categorical or continuous.

    Arguments
    ---------
    rel:
        Relation with the data to operate on.
    attr_cols:
        List of column names in `rel` that are event attributes.

    Notes
    -----
    Columns are classified as categorical if the set of unique values
    in the column matches the set of categories in the .sav metadata.

    In practice, attributes are sequentially classified as:
        - continuous if their metadata has no value label
        - continuous if the either the approximate or exact count of unique
        values is above the number of declared categories
        - categorical if the set of distinct values matches the set of
        declared categories
        - continuous otherwise
    """
    unassigned_cols: set[str] = set()
    cols_to_value_labels: dict[str, dict] = {}
    cols_to_attr_type: dict[str, str] = {}

    for col_meta in attr_col_meta:
        if col_meta.value_labels is None:
            cols_to_attr_type[col_meta.variable] = NoteAttributeType.continuous
            continue
        unassigned_cols.add(col_meta.variable)
        cols_to_value_labels[col_meta.variable] = col_meta.value_labels

    col_queries = [f"approx_count_distinct({col}) AS {col}" for col in cols_to_value_labels]
    approx_distinct_rel = rel.aggregate(", ".join(col_queries)).execute()

    for col, value_labels in cols_to_value_labels.items():
        approx_distinct_count = approx_distinct_rel.select(col).fetchall()[0][0]
        if approx_distinct_count <= len(value_labels):
            continue
        cols_to_attr_type[col] = NoteAttributeType.continuous
        unassigned_cols.remove(col)

    count_distincts = rel.aggregate(f"COUNT(DISTINCT COLUMNS({list(unassigned_cols)}))")

    for col in list(unassigned_cols):
        n_distinct = count_distincts.select(col).fetchall()[0][0]
        if n_distinct <= len(cols_to_value_labels[col]):
            continue
        cols_to_attr_type[col] = NoteAttributeType.continuous
        unassigned_cols.remove(col)

    for remaining_col in list(unassigned_cols):
        uniques = rel.select(remaining_col).distinct().fetchall()
        uniques = [x[0] for x in uniques]
        if set(uniques) == set(cols_to_value_labels[remaining_col].keys()):
            cols_to_attr_type[remaining_col] = NoteAttributeType.categorical
            continue
        cols_to_attr_type[remaining_col] = NoteAttributeType.continuous

    return cols_to_attr_type


def replace_continuous_nulls(rel: DuckDBPyRelation, attr_col_meta: list[SavColumnMeta]) -> DuckDBPyRelation:
    """Replace NULLs as per metadata and cast to DOUBLE.

    Arguments
    ---------
    rel:
        Relation with the data to operate on.
    attr_col_meta:
        Metadata of columns to process.
    """
    for col_meta in attr_col_meta:
        attr_name = col_meta.variable
        if col_meta.value_labels is None:
            logger.debug("No value labels declared for %s", attr_name)
            continue

        missing_values = [float(x) for x in col_meta.value_labels]

        rel = rel.select(f"""
           * EXCLUDE {attr_name},
            CASE WHEN
               {attr_name} IN {missing_values} THEN NULL::DOUBLE
               ELSE {attr_name}::DOUBLE
            END AS {attr_name}
        """)
    return rel


def convert_to_silver(
    source_manifest: SourceManifest, source_path: Path, source_filename: Path, dest_path: Path, config: SilverConfig
) -> SilverMetaRecord:
    """Convert bronze data to silver.

    Arguments
    ---------
    source_path: path to the original .sav file.
    source_filename: name of the original .sav file.
    db_file: path to the database file with the metadata.
    dest_path: Path where to save the silver file.

    Notes
    -----
    The function assumes that:
        - the column declared as person ID can be cast to integer without error
        - the file metadata have a non-NULL `ref_period` if the config defines
        a new event time column.
    """
    design_lookup = {"source_path": source_path, "source_filename": source_filename}
    file_record = FileMetaRecord.from_table(design_lookup, table=source_manifest)
    sav_meta_table = SavMetaTable(db_file=source_manifest.db_file)

    con = duckdb.connect()

    rel = con.read_parquet(file_record.bronze_path)  # type: ignore[arg-type]
    rel = rel.filter(f"{config.rinpersoons_col} == 'R'")
    rel = define_event_time_col(rel, config, file_record)

    rel = rel.select(
        f"* EXCLUDE({config.rinpersoon_col}), CAST({config.rinpersoon_col} AS BIGINT) AS {config.rinpersoon_col}"
    )
    rel = rel.select(f"* EXCLUDE ({','.join(config.cols_to_drop)})")

    columns = rel.columns
    attribute_columns = filterfalse(lambda col: col in config.event_idx_columns, columns)
    attribute_columns_meta = [
        SavColumnMeta.from_table(lookup=design_lookup | {"variable": col}, table=sav_meta_table)
        for col in attribute_columns
    ]

    # Use the full table to avoid any accidental misclassification
    full_table_rel = con.read_parquet(file_record.bronze_path)  # type: ignore[arg-type]
    cols_to_attr_type = classify_attributes(full_table_rel, attribute_columns_meta)

    continuous_col_meta = [
        col_meta
        for col_meta, (col_name, type_) in zip(attribute_columns_meta, cols_to_attr_type.items(), strict=True)
        if type_ == "continuous"
    ]
    rel = replace_continuous_nulls(rel, continuous_col_meta)

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
        bronze_id_col=config.rinpersoon_col,
        bronze_event_time_col=config.event_time_col if config.has_event_col else config.apply_event_time,  # type: ignore[arg-type]
        bronze_cols_dropped=config.cols_to_drop,
        last_modified=datetime.now(UTC),
        event_note_types=cols_to_attr_type,
    )


# Todo
# create quarantine table?
