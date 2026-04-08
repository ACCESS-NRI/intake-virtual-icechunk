# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations


class IcechunkStoreBuilder:
    """Build a virtual Icechunk store from an existing intake-esm datastore.

    Given a pre-built intake-esm catalog, this builder iterates over every
    dataset group in the catalog, opens the constituent files with VirtualiZarr
    to create virtual references, and writes each dataset as a named Zarr
    *group* inside a single Icechunk store.  The result is one Icechunk store
    with one group per dataset, mirroring the logical structure of the
    intake-esm catalog.

    Each group's ``.zattrs`` is populated with the ``groupby_attrs`` values for
    that dataset, so that :class:`~intake_virtual_icechunk.core.IcechunkCatalog`
    can discover, list, and search entries without reading any data arrays.

    After the store is built, a lightweight JSON sidecar is written alongside
    the store so the catalog can be re-opened with
    :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.

    Icechunk session, store, and branch management is handled internally so
    callers need only supply paths.

    Parameters
    ----------
    catalog_path : str
        Path to an existing intake-esm catalog JSON file.
    store_path : str
        Path or URI at which to create (or open) the Icechunk store.
        Supported schemes: local path, ``s3://``, ``gs://`` / ``gcs://``,
        ``az://``.
    storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend.
    """

    def __init__(self, catalog_path, store_path, storage_options=None):
        self.catalog_path = catalog_path
        self.store_path = store_path
        self.storage_options = storage_options or {}

    def build(self):
        """Build the Icechunk store.

        For each dataset group in the intake-esm catalog:

        1. Collects the asset file paths that belong to the group.
        2. Opens each file with VirtualiZarr to create virtual references.
        3. Combines and writes the virtual dataset as a named Zarr group in
           the Icechunk store.
        4. Writes the group's ``groupby_attrs`` values into ``.zattrs``.

        After all groups are written, a JSON sidecar is saved alongside the
        store for use with
        :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.

        The group name for each dataset is derived from the catalog's
        ``groupby_attrs``, so the Icechunk store structure mirrors the
        intake-esm grouping.
        """
        import os

        import icechunk
        import intake
        import virtualizarr  # noqa: F401 – ensure VirtualiZarr is importable
        import xarray as xr
        import zarr

        from ._storage import _resolve_storage
        from .cat import VirtualIcechunkCatalogModel

        # ------------------------------------------------------------------
        # 1. Load the intake-esm catalog
        # ------------------------------------------------------------------
        esm_cat = intake.open_esm_datastore(self.catalog_path)
        esmcat = esm_cat.esmcat

        agg_control = esmcat.aggregation_control
        groupby_attrs = (
            agg_control.groupby_attrs if agg_control else list(esmcat.df.columns)
        )
        assets_col = esmcat.assets.column_name

        # ------------------------------------------------------------------
        # 2. Create the Icechunk repository
        # ------------------------------------------------------------------
        storage = _resolve_storage(self.store_path, self.storage_options)
        repo = icechunk.Repository.create(storage)

        # ------------------------------------------------------------------
        # 3. Build each group inside a single transaction
        # ------------------------------------------------------------------
        group_key_map = esmcat._construct_group_keys()

        with repo.transaction(
            "main", message="Build Virtual Icechunk catalog"
        ) as store:
            for public_key, internal_key in group_key_map.items():
                grouped = esmcat.grouped
                group_df = grouped.get_group(internal_key)

                # Collect group-level metadata for .zattrs
                group_attrs: dict = {}
                for attr in groupby_attrs:
                    if attr in group_df.columns:
                        group_attrs[attr] = group_df[attr].iloc[0]

                # Collect asset file paths for this group
                file_paths: list[str] = group_df[assets_col].tolist()

                # ----------------------------------------------------------
                # VirtualiZarr integration
                # Open each source file as a virtual dataset, combine, and
                # write to the Icechunk store as a Zarr group.
                # ----------------------------------------------------------
                vdatasets = [
                    xr.open_dataset(fp, engine="virtualizarr") for fp in file_paths
                ]
                combined = (
                    xr.combine_by_coords(vdatasets)
                    if len(vdatasets) > 1
                    else vdatasets[0]
                )
                combined.virtualize.to_icechunk(store, group=public_key)

                # Write group metadata into .zattrs so the catalog can search
                # these groups without opening the arrays.
                zarr_group = zarr.open_group(store, path=public_key, mode="a")
                zarr_group.attrs.update(group_attrs)

        # ------------------------------------------------------------------
        # 4. Write the JSON sidecar
        # ------------------------------------------------------------------
        store_path_obj = os.path.abspath(self.store_path)
        sidecar_name = os.path.splitext(os.path.basename(store_path_obj))[0]
        sidecar_dir = os.path.dirname(store_path_obj)

        model = VirtualIcechunkCatalogModel(
            store=self.store_path,
            storage_options=self.storage_options,
        )
        model.save(sidecar_name, directory=sidecar_dir or None)
