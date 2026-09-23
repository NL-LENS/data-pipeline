import logging
from dataclasses import dataclass
from itertools import filterfalse
from pathlib import Path
import duckdb
from data_pipeline.schemas import FileMetaRecord
from data_pipeline.schemas import SavColumnMeta
from data_pipeline.schemas import SavMetaTable
from data_pipeline.schemas import SourceManifest

logger = logging.getLogger(__name__)

TIME_COLNAME = "DATE"
PERSON_COLNAME = "RINPERSOON"
NOTE_COLNAME = "NOTE"


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
        Format of event_time_col (after converting to string).
        Follows the duckdb spec: https://duckdb.org/docs/lts/sql/functions/dateformat#format-specifiers

    Notes
    -----
    Columns are dropped in silver as follows:
        - id_cols that are not rinpersoon_col.
        - time_cols that are not event_time_col.
        - the RINPERSOONS column

    """

    rinpersoon_col: str
    rinpersoons_col: str
    time_cols: list[str]
    id_cols: list[str]
    event_time_col: str | None
    event_time_col_fmt: str = "%Y-%m-%d"

    @property
    def cols_to_drop(self) -> str:
        """Columns to drop from the table."""
        drop_cols: list[str] = []
        drop_cols += filterfalse(lambda x: x == self.event_time_col, self.time_cols)
        drop_cols += filterfalse(lambda x: x == self.rinpersoon_col, self.id_cols)
        drop_cols += [self.rinpersoons_col]
        return ",".join(drop_cols)

    @property
    def event_idx_columns(self) -> list[str | None]:
        """List of columns defining an event."""
        return [self.rinpersoon_col, self.event_time_col]


def convert_to_silver(
    source_manifest: SourceManifest, source_path: Path, source_filename: Path, dest_path: Path, config: SilverConfig
) -> None:
    """Convert bronze data to silver.

    Arguments
    ---------
    source_path: path to the original .sav file.
    source_filename: name of the original .sav file.
    db_file: path to the database file with the metadata.
    dest_path: Path where to save the silver file.

    Notes
    -----
    Current assumptions in the function:
        - the column declared as person ID can be cast to integer without error
    """
    design_lookup = {"source_path": source_path, "source_filename": source_filename}
    file_record = FileMetaRecord.from_table(design_lookup, table=source_manifest)

    sav_meta_table = SavMetaTable(db_file=source_manifest.db_file)

    con = duckdb.connect()
    rel = con.read_parquet(file_record.bronze_path)  # type: ignore[arg-type]

    rel = rel.filter(f"{config.rinpersoons_col} == 'R'")

    rel = rel.select(
        f"* EXCLUDE({config.rinpersoon_col}), CAST({config.rinpersoon_col} AS BIGINT) AS {config.rinpersoon_col}"
    )
    rel = rel.select(f"""* EXCLUDE({config.event_time_col}),
                     try_strptime({config.event_time_col}, '{config.event_time_col_fmt}')::DATE
                     AS {config.event_time_col}""")
    rel = rel.filter(f"{config.event_time_col} IS NOT NULL")

    rel = rel.select(f"* EXCLUDE ({config.cols_to_drop})")

    columns = rel.columns
    attribute_columns = filterfalse(lambda col: col in config.event_idx_columns, columns)

    attribute_col_types = {}
    for attr_col in attribute_columns:
        col_lookup = design_lookup | {"variable": attr_col}
        col_meta = SavColumnMeta.from_table(lookup=col_lookup, table=sav_meta_table)
        value_labels = col_meta.value_labels

        # TODO: use match-case?
        if value_labels is None:  # TODO: is this right?
            attribute_col_types[attr_col] = "continuous"
            continue

        n_labels = len(value_labels.keys())
        n_distinct = rel.select(attr_col).distinct().aggregate("count(*)").fetchall()[0][0]
        if n_distinct > n_labels:
            attribute_col_types[attr_col] = "continuous"
            continue

        distinct_values = rel.select(attr_col).distinct().fetchall()
        distinct_values = [x[0] for x in distinct_values]  # TODO: how to do better?
        if set(distinct_values) == set(value_labels.keys()):
            attribute_col_types[attr_col] = "categorical"
            continue

        attribute_col_types[attr_col] = "continuous"

    # TODO: add here a list of column names in the right order and reuse below?

    # TODO: merge this into the same function as above? ie, to avoid the
    # DB queries a second time?
    for attr_col, type_ in attribute_col_types.items():
        if type_ == "categorical":
            continue

        col_lookup = design_lookup | {"variable": attr_col}
        col_meta = SavColumnMeta.from_table(lookup=col_lookup, table=sav_meta_table)

        if col_meta.value_labels is None:
            logger.debug("No value labels declared for %s", attr_col)
            continue

        missing_values = list(col_meta.value_labels.keys())
        missing_values = [float(x) for x in missing_values]
        rel_with_invalid = rel.filter(f"{attr_col} IN {missing_values}").select(
            f"* EXCLUDE {attr_col}, NULL::DOUBLE AS {attr_col}"
        )
        rel_with_valid = rel.filter(f"{attr_col} NOT IN {missing_values}")

        rel = rel_with_valid.union(rel_with_invalid.select(",".join(rel_with_valid.columns)))

    struct_query_inputs = [f"{col} := {col}" for col in attribute_col_types]
    struct_query = ", ".join(struct_query_inputs)
    rel = rel.select(f"""{config.rinpersoon_col} AS {PERSON_COLNAME},
                     {config.event_time_col} AS {TIME_COLNAME},
                     struct_pack({struct_query}) AS {NOTE_COLNAME}
                     """)

    rel = rel.order(f"{PERSON_COLNAME}, {TIME_COLNAME}")

    rel.to_parquet(str(dest_path))  # TODO: necessary to stream? how?

    con.close()


# Todo
# TODO: create silver schema/table?
# ingested at?
# bronze source?
# categorical/continuous mapping
# else?

# create quarantine table?

# TODO: add test for dict | None type (read function)
# TODO: break out function for type detection; unit-test it
# Add test case where a date column is added - requires modularizing tests?
# sql injection also for rel.filter - how to deal with it? through pydantic?
