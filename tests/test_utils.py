import pytest

from intake_virtual_icechunk.utils import (
    ObjectStoreError,
    _filter_config_args,
    _intake_cat_filename,
    _path_to_url,
    _resolve_store,
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

    def test_file_uri(self):
        # Regression: Path() on POSIX collapses file:///path → file:/path
        result = _sidecar_url("file:///tmp/demo.icechunk")
        assert (
            result == "file:///tmp/demo.icechunk/_intake_demo.json"
        ), f"file:// URI mangled: got {result!r}"

    def test_file_uri_trailing_slash(self):
        result = _sidecar_url("file:///tmp/demo.icechunk/")
        assert result == "file:///tmp/demo.icechunk/_intake_demo.json"

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


class TestPathToUrl:
    def test_bare_absolute_path_gets_file_scheme(self, tmp_path):
        result = _path_to_url(str(tmp_path))
        assert result == f"file://{tmp_path}"

    def test_file_scheme_returned_unchanged(self):
        url = "file:///tmp/my-store.icechunk"
        assert _path_to_url(url) == url

    def test_s3_scheme_returned_unchanged(self):
        url = "s3://my-bucket/prefix/store.icechunk"
        assert _path_to_url(url) == url

    def test_relative_path_is_made_absolute(self):
        """
        Bit hard to assert what the path will become here as it depends on the test
        runner's CWD, but we can at least check that it gets an absolute path and
        the file scheme.
        """
        import os

        result = _path_to_url("relative/path")
        assert result.startswith("file://")
        assert os.path.isabs(result[len("file://") :])


class TestFilterConfigArgs:
    def test_empty_dict_returns_empty(self):
        assert _filter_config_args({}) == {}

    def test_endpoint_url_renamed_to_endpoint(self):
        result = _filter_config_args({"endpoint_url": "https://example.com"})
        assert result == {"endpoint": "https://example.com"}
        assert "endpoint_url" not in result

    def test_anonymous_renamed_to_skip_signature(self):
        result = _filter_config_args({"anonymous": True})
        assert result == {"skip_signature": True}
        assert "anonymous" not in result

    def test_anonymous_false_renamed(self):
        result = _filter_config_args({"anonymous": False})
        assert result == {"skip_signature": False}

    def test_anonymous_absent_skip_signature_not_injected(self):
        # When anonymous is not set, skip_signature must not be added
        result = _filter_config_args({"region": "us-east-1"})
        assert "skip_signature" not in result

    def test_icechunk_specific_keys_dropped(self):
        opts = {"s3_compatible": True, "force_path_style": True, "from_env": True}
        assert _filter_config_args(opts) == {}

    def test_passthrough_keys_preserved(self):
        result = _filter_config_args({"region": "ap-southeast-2", "allow_http": False})
        assert result == {"region": "ap-southeast-2", "allow_http": False}


class TestResolveStore:
    def test_local_path(self, tmp_path):
        result = _resolve_store(f"file://{tmp_path}/", {})
        # Should return an icechunk local filesystem store config (not raise)
        assert result is not None

    def test_s3_url(self):
        result = _resolve_store("s3://my-bucket/", {})
        assert result is not None

    def test_s3_url_with_endpoint(self):
        store_options = {
            "endpoint_url": "https://projects.pawsey.org.au",
            "s3_compatible": True,
            "force_path_style": True,
            "anonymous": True,
        }
        result = _resolve_store(
            "s3://my-bucket/",
            store_options=store_options,
        )
        assert result is not None

    def test_gcs_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="GCS"):
            _resolve_store("gs://my-bucket/", {})

    def test_azure_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="Azure"):
            _resolve_store("az://my-container/", {})
