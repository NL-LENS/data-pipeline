import re
from itertools import filterfalse

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def query_params(data: list | tuple) -> str:
    """Create SQL query parameters for inserting `data`."""
    n_columns = len(data)
    return ",".join(["?"] * n_columns)


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
