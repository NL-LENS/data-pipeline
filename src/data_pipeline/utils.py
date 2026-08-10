import re
from collections.abc import Iterable
from itertools import filterfalse

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def make_insert_params(data: list | tuple) -> str:
    """Create SQL query parameters for inserting `data`."""
    n_columns = len(data)
    return ",".join(["?"] * n_columns)


def where_query_params(column_names: Iterable[str]) -> str:
    """Parameterize SQL string for WHERE query."""
    where = [f"{col} = ?" for col in column_names]
    return " AND ".join(where)


def set_statement(column_names: Iterable[str]) -> str:
    """CREATE parameterized SET statement for an UPDATE query."""
    set_stmt = [f"{col} = ?" for col in column_names]
    return f"SET {', '.join(set_stmt)}"


def quote_identifier(name: str) -> str:
    """Validate and safely quote a SQL identifier.

    To avoid SQL injection risks, validate them against a regex and quote them.

    Raises
    ------
    ValueError if the identifier does not match a conservative allowlist.
    """
    # AI NOTE: suggested and built by LLM.
    if not _VALID_IDENTIFIER.match(name):
        msg = f"Invalid SQL identifier: {name!r}"
        raise ValueError(msg)
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def filter_list(list_in: list, drop_patterns: list) -> list:
    """Filter a list x and drop based on substring matches.

    Arguments
    --------
    list_in: The list to process.
    drop_patterns: A list of patterns to drop. Elements in `list_in` that
    contain a substring matching any of the `drop_patterns` are dropped.

    Returns
    -------
    The modified list.
    """
    return list(filterfalse(lambda x: any(y in x for y in drop_patterns), list_in))
