"""Column names for the silver layer."""

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
        "municipality": "MUNICIPALITY_ID",
        "job": "JOB_ID",
    }
)

SILVER_COL_NAMES = [TIME_COLUMN, NOTE_COLUMN, RELATION_COLUMN, list(ENTITY_COLUMNS.values())]


@dataclass(frozen=True)  # TODO: what for?
class EntityKey:
    """Mapping of silver entity column to constituting bronze columns.

    Arguments
    ---------
    type_:
        Silver entity type as defined in :py:data:`~src.data_pipeline.silver_schema.py`.
    columns:
        List of strings of bronze columns constituting the entity.
    """

    type_: str
    bronze_columns: list[str]

    def make_query(self) -> str:
        """Make struct query for defining this silver entity."""
        if len(self.bronze_columns) == 1:
            return self.bronze_columns[0]

        query_inputs = [f"KEY{i} := {col}" for i, col in enumerate(self.bronze_columns)]
        return f"struct_pack({', '.join(query_inputs)})"


@dataclass
class NoteColumn:
    """Container for creating columns in silver.

    Arguments
    ---------
    columns:
        List of strings of bronze columns constituting the entity.
    """

    columns: list[str]

    @property
    def silver_column(self) -> str:
        """Silver column mapped from entity_type."""
        return NOTE_COLUMN

    def event_table_query(self) -> str:
        """Create query for making event table."""
        query_inputs = [f"{col} := {col}" for col in self.columns]
        return f"struct_pack({', '.join(query_inputs)}) AS {self.silver_column}"


@dataclass
class PrimaryKeyColumn:
    """Container for silver primary key.

    Arguments
    ---------
    entity:
        EntityKey of the column.
    """

    entity: EntityKey

    @property
    def silver_column(self) -> str:
        """Silver column mapped from entity_type."""
        return ENTITY_COLUMNS[self.entity.type_]

    @property
    def bronze_columns(self) -> list[str]:
        """Return constituting bronze columns."""
        return self.entity.bronze_columns

    @classmethod
    def from_dict(cls, entity_dict: dict[str, list[str]]) -> Self:
        """Create instance from dictionary."""
        if len(list(entity_dict.keys())) > 1:
            msg = "Primary key can only be one entity."
            raise RuntimeError(msg)

        type_ = next(iter(entity_dict.keys()))
        columns = next(iter(entity_dict.values()))
        return cls(EntityKey(type_=type_, bronze_columns=columns))

    def event_table_query(self) -> str:
        """Create query for making event table."""
        return f"{self.entity.make_query()} AS {self.silver_column}"


@dataclass
class SecondaryKeyColumn:
    """Container for silver relations column.

    Arguments
    ---------
    entities: list of entities or None
    """

    entities: list[EntityKey] | None

    @classmethod
    def from_list(cls, entity_list: list[dict[str, list[str]]] | None) -> Self:
        """Create class from list of dictionaries."""
        if entity_list is None:
            return cls(None)

        return cls([EntityKey(type_, columns) for entity in entity_list for (type_, columns) in entity.items()])

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
