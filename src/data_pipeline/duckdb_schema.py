import types
from collections.abc import Iterable
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import fields
from datetime import datetime
from itertools import filterfalse
from pathlib import Path
from types import MappingProxyType
from typing import Any
from typing import ClassVar
from typing import NewType
from typing import Self
from typing import get_args
import duckdb
from data_pipeline.utils import make_insert_params
from data_pipeline.utils import quote_identifier
from data_pipeline.utils import set_statement
from data_pipeline.utils import where_query_params

DuckDBBigInt = NewType("DuckDBBigInt", int)
DuckDBDouble = NewType("DuckDBDouble", float)
# UBIGINT, UINTEGER omitted: unclear if necessary


def sql_dict_factory(data: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    """Convert data to dictionary.

    Purposes: use in custom as_dict in data classes, and convert
    Python types where duckdb does not support it (namely: pathlib.Path).

    Arguments
    ---------
    data: Iterable, likely from dict_items.

    """
    out_dict = {}
    for attr, value in data:
        if isinstance(value, Path):
            out_dict[attr] = str(value)
            continue
        out_dict[attr] = value

    return out_dict


@dataclass
class DuckDBRecord:
    """Base class for dataclass-backed records in DuckDB tables.

    Defines a schema of columns and types, and maps from Python's type
    system to DuckDB's, using https://duckdb.org/docs/lts/clients/python/conversion.

    Serves as single source of truth for declaring table schemas, and exploits
    Python's typing system to keep code and schema mutually consistent.

    For ease of use, objects of type pathlib.Path are kept as such
    in Python, but written to DuckDB as strings, using a custom
    conversion function.

    Notes
    -----
    - Numeric Python types are by default converted to 32-bit DuckDB types. For
    using 64-bit precision, declare `DuckDBBigInt` and `DuckDBDouble` in the schema.
    - The class currently cannot deal with special DuckDB data types (LIST, STRUCT, JSON).
    """

    # MappingProxyType provides a read-only interface to the
    # TYPE_MAP dict
    # https://majabojarska.dev/posts/mapping-proxy-type/
    # https://stackoverflow.com/questions/41795116/difference-between-mappingproxytype-and-pep-416-frozendict

    TYPE_MAP: ClassVar[MappingProxyType[Any, str]] = MappingProxyType(
        {
            bool: "BOOLEAN",
            int: "INTEGER",
            str: "VARCHAR",  # TEXT and VARCHAR are aliases
            datetime: "TIMESTAMPTZ",
            float: "FLOAT",
            dict: "JSON",
            Path: "VARCHAR",
            DuckDBBigInt: "BIGINT",
            DuckDBDouble: "DOUBLE",
        }
    )
    DICT_KEY_FOR_PK: ClassVar[str] = "primary_key"

    def __post_init__(self) -> None:
        mapping = self._map_non_union_types() | self._map_union_types()
        # fields(self) ensures correct order in the dict
        self.field_to_type_map: dict[str, str] = {column.name: mapping[column.name] for column in fields(self)}

    def _map_non_union_types(self) -> dict[str, str]:
        """Map non-union types to DuckDB types.

        Raises
        ------
        TypeError if the declared Python type does not map into a
        DuckDB type.
        """
        mapping = {}
        fields_ = [f for f in fields(self) if not isinstance(f.type, types.UnionType)]
        for field_ in fields_:
            py_type = field_.type

            if py_type not in self.TYPE_MAP:
                msg = f"Declared type {py_type} for field {field_.name} has no corresponding type in DuckDB."
                raise TypeError(msg)

            mapping[field_.name] = self.TYPE_MAP[py_type]
        return mapping

    def _map_union_types(self) -> dict[str, str]:
        """Map union types to DuckDB types.

        Uses the first entry in the Union.

        Raises
        ------
        TypeError if none of the members of the Union type are mapped to a
        DuckDB type.
        """
        mapping = {}
        fields_ = [f for f in fields(self) if isinstance(f.type, types.UnionType)]
        for field_ in fields_:
            py_type = field_.type
            type_args = get_args(py_type)
            matching_args = filterfalse(lambda x: x not in self.TYPE_MAP, list(type_args))
            try:
                py_type = next(matching_args)
            except StopIteration as e:
                msg = f"Declared type {py_type} for field {field_.name} has no corresponding type in DuckDB."
                raise TypeError(msg) from e
            mapping[field_.name] = self.TYPE_MAP[py_type]
        return mapping

    def as_dict(self) -> dict[str, Any]:
        """Create dictionary for sql insertion.

        Returns a dictionary, converting values according to the function
        `sql_dict_factory`.

        Notes
        -----
        dataclasses.as_dict maintains the order of fields, ensuring correct
        insertion order when writing to the database.
        See https://stackoverflow.com/questions/76404084/how-can-i-control-the-order-of-attributes-when-converting-dataclasses-to-a-dict.
        """
        # asdict has some gotchas (speed, recursion, deepcopy).
        # Claude argued against asdict but the dict_factory argument
        # gives control; speed may have been improved since and is not critical currently.
        # https://discuss.python.org/t/remove-attributes-from-asdict-method-when-dataclass-field-repr-false/24960
        # https://discuss.python.org/t/dataclasses-make-asdict-astuple-faster-by-skipping-deepcopy-for-objects-where-deepcopy-obj-is-obj/24662/14
        return asdict(self, dict_factory=sql_dict_factory)

    @classmethod
    def py_types(cls) -> dict[str, type]:
        """Return a dict mapping field names to Python types."""
        return {field_.name: getattr(field_, "type") for field_ in fields(cls)}  # noqa: B009 - getattr makes mypy happy

    @classmethod
    def primary_keys(cls) -> list[str]:
        """Return the field names that are defined as primary keys."""
        return [f.name for f in fields(cls) if f.metadata.get(cls.DICT_KEY_FOR_PK)]

    @property
    def key_attributes(self) -> dict[str, Any]:
        """Return dictionary of key-value pairs for the primary keys."""
        return {name: value for name, value in self.as_dict().items() if name in self.primary_keys()}

    @property
    def non_key_attributes(self) -> dict[str, Any]:
        """Return dictionary of key-value pairs for the fields that are not primary keys."""
        return {name: value for name, value in self.as_dict().items() if name not in self.primary_keys()}

    @classmethod
    def from_table(cls, lookup: dict[str, Any], table: "DuckDBTable") -> Self:
        """Instantiate a record from a table.

        Arguments
        ---------
        lookup:
            Mapping from pairs of (column names of primary keys, column values).
            They identify the row to read. Column names must match the record's
            declared primary keys.
        table: Table to read from.

        Notes
        -----
        Values are converted to duckdb-compatible types according
        to `sql_dict_factory`.
        """
        required_keys = cls.primary_keys()
        if set(required_keys) != set(lookup.keys()):
            msg = f"The record has keys {cls.primary_keys()} but {lookup.keys()} where declared."
            raise RuntimeError(msg)

        lookup_cast = sql_dict_factory(lookup.items())

        data = table.read(lookup_cast, cls.py_types())
        return cls(**data)


@dataclass
class DuckDBTable:
    """Table for duckdb."""

    table_name: str
    db_file: Path | str = ""

    def build_ddl(self, column_definition: dict[str, str], primary_keys: list[str]) -> str:
        """Generate data definition language for this schema.

        Arguments
        ---------
        column_definition:
            dictionary of colum names and DuckDB column types.
        primary_keys:
            list of column names defining the (composite) primary key of the column.
        """
        column_declaration = [
            f"{quote_identifier(col_name)} {col_type}" for col_name, col_type in column_definition.items()
        ]
        column_declaration_str = ", ".join(column_declaration)
        if len(primary_keys) == 0:
            schema = column_declaration_str
        else:
            primary_keys = [quote_identifier(key) for key in primary_keys]
            schema = column_declaration_str + f", PRIMARY KEY ({', '.join(primary_keys)})"
        return f"CREATE TABLE IF NOT EXISTS {quote_identifier(self.table_name)} ({schema})"

    def create(self, column_definition: dict[str, str], primary_keys: list[str]) -> None:
        """Create table with the schema.

        Arguments
        ---------
        column_definition:
            dictionary of column names and DuckDB data types.
        primary_keys:
            list of primary key columns. To create a table without primary keys,
            this should be a list of length 0.

        Note
        ----
        This calls `self.build_ddl` under the hood and thus executes a
        CREATE TABLE IF NOT EXISTS statement. As a result, schema differences between
        the record and the table are not detected and the existing schema
        in the table takes precedence.
        """
        with duckdb.connect(self.db_file) as con:
            sql = self.build_ddl(column_definition, primary_keys)
            con.execute(sql)

    def create_from_record(self, record: DuckDBRecord) -> None:
        """Create a table with the schema defined in the record.

        Convenience wrapper around `create_table`.
        """
        column_definition = record.field_to_type_map
        primary_keys = record.primary_keys()
        self.create(column_definition, primary_keys)

    def insert(self, record: DuckDBRecord) -> None:
        """Insert single record to the table."""
        with duckdb.connect(self.db_file) as con:
            con.sql("SET TIMEZONE='UTC'")
            data_to_insert = record.as_dict()
            insert_params = make_insert_params(list(data_to_insert.values()))
            sql = f"INSERT INTO {quote_identifier(self.table_name)} VALUES ({insert_params})"  # noqa: S608
            con.execute(sql, data_to_insert.values())

    def insert_many(self, records: Iterable[DuckDBRecord]) -> None:
        """Insert many records to the table."""
        with duckdb.connect(self.db_file) as con:
            values_to_insert = [r.as_dict().values() for r in records]
            insert_params = make_insert_params(list(values_to_insert[0]))
            con.executemany(
                f"INSERT INTO {quote_identifier(self.table_name)} VALUES ({insert_params})",  # noqa: S608
                values_to_insert,
            )

    def read(self, lookup: dict[str, Any], column_types: dict[str, type]) -> dict[str, Any]:
        """Read one record from table.

        Arguments
        ---------
        lookup:
            key-value pairs where keys are columns of the table's
            PRIMARY KEY, and values are the column values.
        column_types: mapping of column names to Python types.

        Returns
        -------
        A dictionary of column names and type-casted column values.
        Specifically for Union types declared in the record's field types,
        non-NULL values are cast to the first matching Python type.
        """
        with duckdb.connect(self.db_file, read_only=True) as con:
            con.sql("SET TIMEZONE='UTC'")
            where_params = where_query_params(lookup)
            data = con.execute(f"SELECT * FROM '{self.table_name}' WHERE {where_params}", lookup.values()).fetchone()  # noqa: S608

        if data is None:
            msg = "No records in database for this primary key."
            raise RuntimeError(msg)

        result = {}
        for (column, type_), value in zip(column_types.items(), data, strict=True):
            match (value, type_):
                case (None, _):  # case: value is None
                    result[column] = value
                case (_, types.UnionType()):  # case: type_ is types.UnionType
                    castable_types = filterfalse(lambda x: x is types.NoneType, get_args(column_types[column]))
                    try:
                        casted_value = next(castable_types)(value)
                    except StopIteration as e:
                        msg = f"Value {value} in column {column} cannot be casted to any Python types."
                        raise RuntimeError(msg) from e
                    result[column] = casted_value
                case (v, t) if isinstance(v, t):  # case: value is of type type_,
                    result[column] = v
                case (v, t):  # always true; bind value and type to v, t to re-use in expression
                    result[column] = t(v)

        return result

    def update(self, record: DuckDBRecord) -> None:
        """Update a record in the database.

        Assumes the record exists and is identified via the
        primary key.
        """
        with duckdb.connect(self.db_file) as con:
            lookup = record.key_attributes
            data_to_update = record.non_key_attributes

            set_stmt = set_statement(data_to_update.keys())
            where_params = where_query_params(lookup)
            sql = f"UPDATE '{self.table_name}' {set_stmt} WHERE {where_params}"
            query_data = list(data_to_update.values()) + list(lookup.values())
            con.execute(sql, query_data)
