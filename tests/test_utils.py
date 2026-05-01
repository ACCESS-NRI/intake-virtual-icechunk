import pytest

from intake_virtual_icechunk.utils import (
    ObjectStoreError,
    _intake_cat_filename,
    _resolve_vcc_store,
    _sidecar_url,
)


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

    # Cloud URIs must not mangle the ``://`` separator
    assert (
        _intake_cat_filename("s3://my-bucket/my-catalog.icechunk")
        == "_intake_my-catalog.json"
    )
    assert (
        _intake_cat_filename("gs://my-bucket/catalog.icechunk/")
        == "_intake_catalog.json"
    )


class TestSidecarUrl:
    def test_local_path(self, tmp_path):
        result = _sidecar_url(str(tmp_path / "my.icechunk"))
        assert result.endswith("_intake_my.json")
        # Must not contain ``://``-style corruption
        assert "://" not in result or result.startswith("file://")

    def test_s3_url(self):
        result = _sidecar_url("s3://my-bucket/prefix/catalog.icechunk")
        assert result == "s3://my-bucket/prefix/catalog.icechunk/_intake_catalog.json"

    def test_s3_url_trailing_slash(self):
        result = _sidecar_url("s3://my-bucket/prefix/catalog.icechunk/")
        assert result == "s3://my-bucket/prefix/catalog.icechunk/_intake_catalog.json"

    def test_gcs_url(self):
        result = _sidecar_url("gs://my-bucket/catalog.icechunk")
        assert result == "gs://my-bucket/catalog.icechunk/_intake_catalog.json"

    def test_azure_url(self):
        result = _sidecar_url("az://my-container/catalog.icechunk")
        assert result == "az://my-container/catalog.icechunk/_intake_catalog.json"


class TestResolveVccStore:
    def test_local_path(self, tmp_path):
        result = _resolve_vcc_store(f"file://{tmp_path}/", {})
        # Should return an icechunk local filesystem store config (not raise)
        assert result is not None

    def test_s3_url(self):
        result = _resolve_vcc_store("s3://my-bucket/", {})
        assert result is not None

    def test_s3_url_with_endpoint(self):
        result = _resolve_vcc_store(
            "s3://my-bucket/",
            {"endpoint_url": "https://projects.pawsey.org.au", "allow_http": False},
        )
        assert result is not None

    def test_s3_credentials_not_forwarded(self):
        # Credential keys must be filtered out; we just check that it doesn't raise
        result = _resolve_vcc_store(
            "s3://my-bucket/",
            {
                "access_key_id": "AKID",
                "secret_access_key": "SECRET",
                "endpoint_url": "https://example.com",
            },
        )
        assert result is not None

    def test_gcs_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="GCS"):
            _resolve_vcc_store("gs://my-bucket/", {})

    def test_gcs_alt_scheme_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="GCS"):
            _resolve_vcc_store("gcs://my-bucket/", {})

    def test_azure_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="Azure"):
            _resolve_vcc_store("az://my-container/", {})

    def test_unknown_scheme_raises(self):
        with pytest.raises(ObjectStoreError, match="Unsupported URL prefix scheme"):
            _resolve_vcc_store("ftp://some-server/path/", {})
