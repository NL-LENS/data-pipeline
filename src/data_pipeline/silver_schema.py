"""Column names for the silver layer."""

from collections.abc import Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Self

TIME_COLUMN = "DATE"
NOTE_COLUMN = "NOTE"
RELATION_COLUMN = "RELATIONS"

# mapping of silver entity types to column names
ENTITY_COLUMNS: MappingProxyType[str, str] = MappingProxyType(
    {
        "person": "PERSON_ID",
        "company": "COMPANY_ID",
        "school": "SCHOOL_ID",
        "dwelling": "DWELLING_ID",
        "household": "HOUSEHOLD_ID",
        "municipality": "MUNICIPALITY_ID",
        "job": "JOB_ID",
    }
)

SILVER_COL_NAMES = [TIME_COLUMN, NOTE_COLUMN, RELATION_COLUMN, list(ENTITY_COLUMNS.values())]


@dataclass
class EntityKey:
    """Mapping of silver entity column to constituting bronze columns.

    Parameters
    ----------
    type_
        Silver entity type as defined in :py:data:`~data_pipeline.silver_schema.py`.
    columns
        List of strings of bronze columns constituting the entity.
    """

    type_: str
    bronze_columns: Sequence[str]

    def make_query(self) -> str:
        """Make struct query for defining this silver entity."""
        if len(self.bronze_columns) == 1:
            return self.bronze_columns[0]

        query_inputs = [f"KEY{i} := {col}" for i, col in enumerate(self.bronze_columns)]
        return f"struct_pack({', '.join(query_inputs)})"


@dataclass
class NoteColumn:
    """Container for creating columns in silver.

    Parameters
    ----------
    columns
        List of strings of bronze columns constituting the entity.
    """

    columns: list[str]

    @property
    def silver_column(self) -> str:
        """Silver column mapped from entity_type."""
        return NOTE_COLUMN

    def event_table_query(self) -> str:
        """Create query for making event table."""
        if len(self.columns) == 0:
            return f"NULL AS {self.silver_column}"

        query_inputs = [f"{col} := {col}" for col in self.columns]
        return f"struct_pack({', '.join(query_inputs)}) AS {self.silver_column}"


@dataclass
class PrimaryKeyColumn:
    """Container for silver primary key.

    Parameters
    ----------
    entity
        EntityKey of the column.
    """

    entity: EntityKey

    @property
    def silver_column(self) -> str:
        """Silver column mapped from entity_type."""
        return ENTITY_COLUMNS[self.entity.type_]

    @property
    def bronze_columns(self) -> Sequence[str]:
        """Return constituting bronze columns."""
        return self.entity.bronze_columns

    @classmethod
    def from_dict(cls, entity_dict: dict[str, list[str] | str]) -> Self:
        """Create instance from dictionary."""
        if len(list(entity_dict.keys())) > 1:
            msg = "Primary key can only be one entity."
            raise RuntimeError(msg)

        type_ = next(iter(entity_dict.keys()))
        columns = next(iter(entity_dict.values()))
        columns = [columns] if isinstance(columns, str) else columns
        return cls(EntityKey(type_=type_, bronze_columns=columns))

    def event_table_query(self) -> str:
        """Create query for making event table."""
        return f"{self.entity.make_query()} AS {self.silver_column}"


@dataclass
class SecondaryKeyColumn:
    """Container for silver relations column.

    Parameters
    ----------
    entities
        list of entities or None
    """

    entities: list[EntityKey] | None

    @classmethod
    def from_dict(cls, entity_dict: dict[str, list[str] | str] | None) -> Self:
        """Initiate class from list of dictionaries."""
        if entity_dict is None:
            return cls(None)

        entities = []
        for type_, columns in entity_dict.items():
            column_list = [columns] if isinstance(columns, str) else columns
            entities.append(EntityKey(type_, column_list))

        return cls(entities)

    @property
    def silver_column(self) -> str:
        """Silver column name."""
        return RELATION_COLUMN

    @property
    def bronze_columns(self) -> list[str] | None:
        """Return constituting bronze columns."""
        if self.entities is None:
            return None

        return [col for entity in self.entities for col in entity.bronze_columns]

    def event_table_query(self) -> str:
        """Create query for making event table."""
        if self.entities is None:
            return f"NULL AS {RELATION_COLUMN}"

        query_list = [f"{ENTITY_COLUMNS[entity.type_]} := {entity.make_query()}" for entity in self.entities]

        return f"struct_pack({', '.join(query_list)}) AS {RELATION_COLUMN}"

    def to_dict(self) -> dict[str, list[str]] | None:
        """Return a dict of entitye type to bronze columns used."""
        if self.entities is None:
            return None
        return {e.type_: list(e.bronze_columns) for e in self.entities}
