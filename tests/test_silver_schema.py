import pytest
from data_pipeline.silver_schema import ENTITY_COLUMNS
from data_pipeline.silver_schema import NOTE_COLUMN
from data_pipeline.silver_schema import RELATION_COLUMN
from data_pipeline.silver_schema import EntityKey
from data_pipeline.silver_schema import NoteColumn
from data_pipeline.silver_schema import PrimaryKeyColumn
from data_pipeline.silver_schema import SecondaryKeyColumn


class TestSilverSchema:
    """Test silver_schema.py."""

    @pytest.mark.parametrize(
        ("entity", "expected"),
        [
            (EntityKey("person", ["RINPERSOON"]), f"RINPERSOON AS {ENTITY_COLUMNS['person']}"),
            (
                EntityKey("school", ["school_id", "location"]),
                f"struct_pack(KEY0 := school_id, KEY1 := location) AS {ENTITY_COLUMNS['school']}",
            ),
        ],
        ids=["single_key", "composite_key"],
    )
    def test_primary_key(self, entity: EntityKey, expected: str):
        """Test creation of primary key column."""
        column = PrimaryKeyColumn(entity)
        result = column.event_table_query()
        assert result == expected

    def test_note_column(self):
        """Test creation of note column."""
        column = NoteColumn(["attr1", "attr2"])
        result = column.event_table_query()
        assert result == f"struct_pack(attr1 := attr1, attr2 := attr2) AS {NOTE_COLUMN}"

    @pytest.mark.parametrize(
        ("entities", "expected"),
        [
            (None, "NULL"),
            ([EntityKey("person", ["RINPERSOON"])], f"struct_pack({ENTITY_COLUMNS['person']} := RINPERSOON)"),
            (
                [EntityKey("school", ["school_id", "location"])],
                f"struct_pack({ENTITY_COLUMNS['school']} := struct_pack(KEY0 := school_id, KEY1 := location))",
            ),
            (
                [EntityKey("person", ["RINPERSOON"]), EntityKey("company", ["SBEID"])],
                f"struct_pack({ENTITY_COLUMNS['person']} := RINPERSOON, {ENTITY_COLUMNS['company']} := SBEID)",
            ),
            (
                [EntityKey("school", ["school_id", "location"]), EntityKey("company", ["firm_id", "branch_id"])],
                (
                    f"struct_pack("
                    f"{ENTITY_COLUMNS['school']} := struct_pack(KEY0 := school_id, KEY1 := location)"
                    f", {ENTITY_COLUMNS['company']} := struct_pack(KEY0 := firm_id, KEY1 := branch_id)"
                    ")"
                ),
            ),
            (
                [EntityKey("school", ["school_id", "location"]), EntityKey("company", ["SBEID"])],
                (
                    f"struct_pack("
                    f"{ENTITY_COLUMNS['school']} := struct_pack(KEY0 := school_id, KEY1 := location)"
                    f", {ENTITY_COLUMNS['company']} := SBEID"
                    ")"
                ),
            ),
        ],
        ids=[
            "none",
            "one_with_single_key",
            "one_with_composite_key",
            "two_with_single_key",
            "two_with_composite_key",
            "two_mixed",
        ],
    )
    def test_secondary_key_column(self, entities: list[EntityKey] | None, expected: str):
        """Test creation of relations column."""
        column = SecondaryKeyColumn(entities)
        result = column.event_table_query()
        expected += f" AS {RELATION_COLUMN}"
        assert result == expected
