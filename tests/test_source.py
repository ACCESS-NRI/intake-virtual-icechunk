# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import xarray as xr

from intake_virtual_icechunk._source import IcechunkDataSource


class TestIcechunkDataSource:
    def test_close_clears_cached_dataset(self, monkeypatch):
        opened = []

        def fake_open_dataset(self):
            ds = xr.Dataset(attrs={"open_count": len(opened)})
            opened.append(ds)
            return ds

        monkeypatch.setattr(IcechunkDataSource, "_open_dataset", fake_open_dataset)
        source = IcechunkDataSource(key="key", store=object(), group="group")

        first = source.ds
        assert source.ds is first
        assert source._ds is first
        assert source.__dict__["ds"] is first

        source.close()

        assert source._ds is None
        assert "ds" not in source.__dict__
        assert source._schema is None

        second = source.ds
        assert second is not first
        assert second.attrs["open_count"] == 1
        assert len(opened) == 2
