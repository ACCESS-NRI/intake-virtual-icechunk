from intake_virtual_icechunk.utils import _intake_cat_filename


def test__intake_cat_filename():
    assert _intake_cat_filename("/path/to/store") == "_intake_store.json"
    assert (
        _intake_cat_filename("/path/to/another_store.icechunk")
        == "_intake_another_store.json"
    )

    assert (
        _intake_cat_filename("/path/to/store.with.dots.icechunk")
        == "_intake_store.with.dots.json"
    )
