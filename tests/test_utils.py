from data_pipeline.metadata import filter_list


def test_filter_list():
    """Test filter_list."""
    list_in = ["some_dir", "another_dir", "Maatwerk", "geconverteerde Daten"]
    expected = ["some_dir", "another_dir"]
    output = filter_list(list_in, ["Maatwerk", "geconverteerde"])
    assert set(expected) == set(output)
