import types
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
from typing import get_args
import duckdb
from data_pipeline.utils import query_params
from data_pipeline.utils import quote_identifier

DuckDBBigInt = NewType("DuckDBBigInt", int)
DuckDBDouble = NewType("DuckDBDouble", float)
# UBIGINT, UINTEGER omitted: unclear if necessary


def sql_dict_factory(data: list[tuple[str, Any]]) -> dict[str, Any]:
    """dict_factory function for custom as_dict."""
    out_dict = {}
    for attr, value in data:
        if isinstance(value, Path):
            out_dict[attr] = str(value)
            continue
        out_dict[attr] = value

    return out_dict


@dataclass
class DuckDBSchema:
    """Base class for dataclass-backed DuckDB schemas.

    Defines a schema of columns and types, and maps from Python's type
    system to DuckDB's, using https://duckdb.org/docs/lts/clients/python/conversion.

    Serves as single source of truth for declaring table schemas, and exploits
    Python's typing system to keep code and schema mutually consistent.

    For ease of use, objects of type pathlib.Path are kept as such
    in Python, but written to DuckDB as strings, using a custom
    conversion function.

    Notes
    -----
    - Numeric Python types are by default convertd to 32-bit DuckDB types. For
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

    def __post_init__(self) -> None:
        self.field_to_type_map: dict[str, str] = self._map_non_union_types() | self._map_union_types()

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
            try:  # TODO: fix control flow when the first arg does not match but the second does
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

    def build_ddl(self, table_name: str) -> str:
        """Generate data definition language for this schema."""
        column_declaration = [f"{quote_identifier(f.name)} {self.field_to_type_map[f.name]}" for f in fields(self)]
        return f"CREATE TABLE IF NOT EXISTS {quote_identifier(table_name)} ({', '.join(column_declaration)})"

    def create_table(self, table_name: str, db_file: Path | str = "") -> None:
        """Create table with the schema."""
        with duckdb.connect(db_file) as con:
            sql = self.build_ddl(table_name)
            con.execute(sql)

    def write_to_db(self, table_name: str, db_file: Path | str = "") -> None:
        """Write record to database."""
        with duckdb.connect(db_file) as con:
            con.sql("SET TIMEZONE='UTC'")
            data_to_insert = self.as_dict()
            insert_params = query_params(list(data_to_insert.values()))
            sql = f"INSERT INTO {quote_identifier(table_name)} VALUES ({insert_params})"  # noqa: S608
            con.execute(sql, data_to_insert.values())
