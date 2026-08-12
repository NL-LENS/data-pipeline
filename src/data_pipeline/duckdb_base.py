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
from data_pipeline.utils import is_optional
from data_pipeline.utils import is_union
from data_pipeline.utils import make_insert_params
from data_pipeline.utils import quote_identifier
from data_pipeline.utils import set_statement
from data_pipeline.utils import where_query_params

DuckDBBigInt = NewType("DuckDBBigInt", int)
DuckDBDouble = NewType("DuckDBDouble", float)
# UBIGINT, UINTEGER omitted: unclear if necessary


@dataclass
class ForeignKey:
    """Container for foreign keys.

    Arguments
    ---------
    table:
        The reference table to which the foreign key refer to.
    self_cols:
        The column names in the table that reference the foreign key. The
        order of the columns matters.
    ref_cols:
        The column names in the reference table. The order of the columns
        matters.
    """

    table: str | None
    self_cols: tuple[str, ...]
    ref_cols: tuple[str, ...]

    def build_fk_ddl(self) -> str:
        """Build DDL for FOREIGN KEY part."""
        if self.table is None:
            return ""

        self_tuple = f"({', '.join(self.self_cols)})"
        ref_tuple = f"({', '.join(self.ref_cols)})"
        return f"FOREIGN KEY {self_tuple} REFERENCES {self.table}{ref_tuple}"

    @classmethod
    def from_dict(cls, fk_dict: dict[str, dict[str, str]]) -> Self:
        """Create an instance from a dictionary."""
        tables = [col_key_dict["table"] for col_key_dict in fk_dict.values()]
        if len(set(tables)) > 1:
            msg = "Need single reference table"
            raise RuntimeError(msg)

        ref_cols = [col_key_dict["column"] for col_key_dict in fk_dict.values()]

        return cls(table=tables[0], self_cols=tuple(fk_dict.keys()), ref_cols=tuple(ref_cols))


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

    Serves as single source of truth for declaring record fields, which correspond
    to DuckDB columns. The conversion exploits Python's typing system to keep
    code and schema mutually consistent. Optional columns are supported, but
    it is strongly advised to only use UnionTypes composed of `NoneType` and
    one other type:
    DuckDB columns can only ever be of one type, so while the class supports
    declaring multiple types that are not NoneType, this is not intended usage
    and may lead to bugs.

    For ease of use, objects of type pathlib.Path are kept as such
    in Python, but written to DuckDB as strings, using a custom
    conversion function.

    Primary and foreign keys can be specified with the fields' metadata, and
    are passed to the respective table. Composite keys are supported.
    - To set a field as primary key, use `metadata={"primary_key": True}`.
    - To set a field as referring a foreign key, use
    `metadata={"foreign_key": {"table": "table_name_ref_table", "column": "column_name"}}`.
    - The order of the fields matters for composite keys: for setting a key
    on columns (a, b), the field a should be define before the field b.

    Invalid keys (such as that a foreign key requires a primary key in the reference table)
    are only caught at runtime by DuckDB and raise errors.

    Notes
    -----
    - Numeric Python types are by default converted to 32-bit DuckDB types. For
    using 64-bit precision, declare `DuckDBBigInt` and `DuckDBDouble` in the schema.
    - The class currently cannot deal with special DuckDB data types (LIST, STRUCT, JSON).

    References
    ----------
    - https://duckdb.org/docs/lts/sql/statements/create_table
    - https://duckdb.org/docs/current/sql/constraints
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
    DICT_KEY_FOR_FK: ClassVar[str] = "foreign_key"

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
        fields_ = [f for f in fields(self) if not is_union(f.type) and not is_optional(f.type)]
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
        fields_ = [f for f in fields(self) if is_union(f.type) or is_optional(f.type)]
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
    def primary_key(cls) -> list[str]:
        """Return the field names that are defined as primary keys."""
        return [f.name for f in fields(cls) if f.metadata.get(cls.DICT_KEY_FOR_PK)]

    @classmethod
    def foreign_key(cls) -> ForeignKey | None:
        """Return the field names that are defined as foreign keys."""
        foreign_key_dict = {
            f.name: f.metadata.get(cls.DICT_KEY_FOR_FK, "")
            for f in fields(cls)
            if f.metadata.get(cls.DICT_KEY_FOR_FK, "")
        }
        if len(foreign_key_dict) == 0:
            return None
        return ForeignKey.from_dict(foreign_key_dict)

    @property
    def key_attributes(self) -> dict[str, Any]:
        """Return dictionary of key-value pairs for the primary keys."""
        return {name: value for name, value in self.as_dict().items() if name in self.primary_key()}

    @property
    def non_key_attributes(self) -> dict[str, Any]:
        """Return dictionary of key-value pairs for the fields that are not primary keys."""
        return {name: value for name, value in self.as_dict().items() if name not in self.primary_key()}

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
        required_keys = cls.primary_key()
        if set(required_keys) != set(lookup.keys()):
            msg = f"The record has keys {cls.primary_key()} but {lookup.keys()} where declared."
            raise RuntimeError(msg)

        lookup_cast = sql_dict_factory(lookup.items())

        data = table.read(lookup_cast, cls.py_types())
        return cls(**data)


@dataclass
class DuckDBTable:
    """Table for duckdb."""

    db_file: Path | str = ""
    table_name: ClassVar[str]

    def build_ddl(
        self,
        column_definition: dict[str, str],
        primary_key: list[str] | None = None,
        foreign_key: ForeignKey | None = None,
    ) -> str:
        """Generate data definition language for this schema.

        Arguments
        ---------
        column_definition:
            dictionary of colum names and DuckDB column types.
        primary_key:
            list of column names defining the (composite) primary key of the column.
            If `None` (default), no primary key is used.
        foreign_key:
            An instance of ForeignKey, or None.
            If `None` (default), no foreign key is used.

        Notes
        -----
        For compatibility with the foreign_key and primary_key method on
        DuckDBRecords, the arguments also accept empty list/dicts, which
        are treated in the same way as `None`.
        """
        column_declaration = [
            f"""{quote_identifier(col_name)} {col_type}""" for col_name, col_type in column_definition.items()
        ]
        column_declaration_str = ", ".join(column_declaration)
        if primary_key is None or len(primary_key) == 0:
            schema = column_declaration_str
        else:
            primary_key = [quote_identifier(key) for key in primary_key]
            schema = column_declaration_str + f", PRIMARY KEY ({', '.join(primary_key)})"

        if foreign_key:
            fk_ddl = foreign_key.build_fk_ddl()
            schema = f"{schema}, {fk_ddl}"

        return f"CREATE TABLE IF NOT EXISTS {quote_identifier(self.table_name)} ({schema})"

    def create(
        self,
        column_definition: dict[str, str],
        primary_key: list[str] | None = None,
        foreign_key: ForeignKey | None = None,
    ) -> None:
        """Create table with the schema.

        Arguments
        ---------
        column_definition:
            dictionary of column names and DuckDB data types.
        primary_key:
            list of primary key columns.
            If `None` (default), no primary key is created.
        foreign_key:
            An instance of ForeignKey, or None.
            If `None` (the default), no foreign key is created.

        Note
        ----
        This calls `self.build_ddl` under the hood and thus executes a
        CREATE TABLE IF NOT EXISTS statement. As a result, schema differences between
        the record and the table are not detected and the existing schema
        in the table takes precedence.
        """
        with duckdb.connect(self.db_file) as con:
            sql = self.build_ddl(column_definition, primary_key, foreign_key)
            con.execute(sql)

    def create_from_record(self, record: DuckDBRecord) -> None:
        """Create a table with the schema defined in the record.

        Convenience wrapper around `create_table`.
        """
        column_definition = record.field_to_type_map
        primary_key = record.primary_key()
        foreign_key = record.foreign_key()
        self.create(column_definition, primary_key, foreign_key)

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

        result: dict[str, Any] = {}
        for (column, type_), value in zip(column_types.items(), data, strict=True):
            match (value, type_):
                case (None, _):  # case: value is None
                    result[column] = value
                case _ if isinstance(
                    value, datetime
                ):  # when datetime is read from table but declared type can be datetime | None
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
