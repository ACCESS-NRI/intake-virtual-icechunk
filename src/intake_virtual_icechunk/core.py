# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from intake.source.base import DataSource, Schema


class IcechunkSource(DataSource):
    """Intake driver for reading a virtual Icechunk store built from an intake-esm catalog.

    The store is expected to have been created by
    :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`, which writes one
    Zarr group per dataset into a single Icechunk store, mirroring the grouping
    structure of the original intake-esm catalog.

    This driver can open a single group (dataset) at a time.  Use
    :meth:`groups` to discover available groups before opening one.

    Registered as the ``virtual_icechunk`` intake driver, so it is accessible
    via ``intake.open_virtual_icechunk()``.

    Parameters
    ----------
    store : str
        Path or URI to the Icechunk store.
    group : str, optional
        Zarr group path within the store to open.  If ``None``, the root
        group is opened (suitable when the store contains only one dataset).
    storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend.
    xarray_kwargs : dict, optional
        Keyword arguments forwarded to ``xarray.open_zarr()``.
    """

    name = "virtual_icechunk"
    container = "xarray"
    partition_access = False
    version = "0.0.1"

    def __init__(
        self, store, group=None, storage_options=None, xarray_kwargs=None, metadata=None
    ):
        self.store = store
        self.group = group
        self.storage_options = storage_options or {}
        self.xarray_kwargs = xarray_kwargs or {}
        super().__init__(metadata=metadata)

    def _get_schema(self):
        return Schema(
            datashape=None,
            dtype=None,
            shape=None,
            npartitions=1,
            extra_metadata=self.metadata or {},
        )

    def _get_partition(self, i):
        return self.read()

    def read(self):
        """Open a Zarr group from the Icechunk store as an ``xarray.Dataset``.

        Opens ``self.group`` within the store at ``self.store``.  If
        ``self.group`` is ``None``, the root group is opened.
        """
        raise NotImplementedError

    def groups(self):
        """List the available Zarr groups (datasets) in the Icechunk store.

        Returns
        -------
        list of str
            Group paths, one per dataset written by
            :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`.
        """
        raise NotImplementedError

    def _close(self):
        pass
