"""Column names for the silver layer."""

from types import MappingProxyType

TIME_COLUMN = "DATE"
NOTE_COLUMN = "NOTE"

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
