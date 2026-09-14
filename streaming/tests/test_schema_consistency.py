from pulseguard_streaming.feature_state import FEATURE_NAMES
from pulseguard_streaming.output_schema import FEATURE_FIELD_TYPES, OUTPUT_COLUMN_NAMES, OUTPUT_SCHEMA


def test_output_schema_has_a_spark_type_for_every_feature():
    assert set(FEATURE_NAMES) == set(FEATURE_FIELD_TYPES.keys())


def test_output_schema_column_names_are_unique_and_complete():
    assert len(OUTPUT_COLUMN_NAMES) == len(set(OUTPUT_COLUMN_NAMES))
    assert set(FEATURE_NAMES) <= set(OUTPUT_COLUMN_NAMES)
    assert len(OUTPUT_SCHEMA.fields) == len(OUTPUT_COLUMN_NAMES)
