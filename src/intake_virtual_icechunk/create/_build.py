from __future__ import annotations

# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import TYPE_CHECKING

import icechunk
import intake
import zarr
from intake_esm.core import esm_datastore
from virtualizarr import open_virtual_mfdataset

from .._storage import _resolve_storage, _resolve_store
from ..cat import VirtualIcechunkCatalogModel

if TYPE_CHECKING:
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr.parsers import (
        DMRPPParser,
        FITSParser,
        HDFParser,
        KerchunkJSONParser,
        KerchunkParquetParser,
        NetCDF3Parser,
        ZarrParser,
    )

    VirtualizarrParser = (
        type[DMRPPParser]
        | type[FITSParser]
        | type[HDFParser]
        | type[KerchunkJSONParser]
        | type[KerchunkParquetParser]
        | type[NetCDF3Parser]
        | type[ZarrParser]
    )


class IcechunkBuildError(Exception):
    """Custom exception for errors during the Icechunk store build process."""

    pass


class ParserInferenceError(IcechunkBuildError):
    """Raised when the parser cannot be inferred from the intake-esm catalog."""

    pass


class IcechunkStoreBuilder:
    """Build a virtual Icechunk store from an existing intake-esm datastore.

    Given a pre-built intake-esm catalog, this builder iterates over every
    dataset group in the catalog, opens the constituent files with VirtualiZarr to create virtual references, and writes each dataset as a named Zarr
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

    Warning: store_options are for the source data store, not the target Icechunk store.
    The target Icechunk store is always created with default options.
    If you need to customize the target store (e.g. for a non-file-based storage backend),
    please open an issue or submit a PR.

    Parameters
    ----------
    catalog_path : str
        Path to an existing intake-esm catalog JSON file.
    store_path : str
        Path or URI at which to create (or open) the Icechunk store.
        Supported schemes: local path, ``s3://``, ``gs://`` / ``gcs://``,
        ``az://``.
    parser : VirtualizarrParser, optional
        Optionally specify the VirtualiZarr parser to use when opening source. I
        not provided, the builder will attempt to infer the parser from the intake-esm
        catalog's ``assets.format`` field. See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec
    storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend. See _resolve_storage() for details.
    store_options: dict, optional

    """

    def __init__(
        self,
        catalog_path: str,
        store_path: str,
        parser: VirtualizarrParser | None = None,
        storage_options: dict | None = None,
        store_options: dict | None = None,
    ):
        self.catalog_path = catalog_path
        self.store_path = store_path
        self._esm_ds = None

        self.storage_options = storage_options or {}
        self.store_options = store_options or {}
        parser = parser or self._infer_parser()
        self.parser = parser()

    @property
    def esm_ds(self) -> esm_datastore:
        """
        Use a property to lazily load the intake-esm datastore only when needed,
        and cache it on the builder instance. mostly so we can put all our optional
        arguments in `__init__` together and not have to worry about ordering.
        """

        self._esm_ds: esm_datastore | None
        if self._esm_ds is None:
            self._esm_ds = intake.open_esm_datastore(self.catalog_path)
        return self._esm_ds

    def _infer_parser(self) -> VirtualizarrParser:
        """
        We can infer the parser from the esm datastore, since it's specification
        contains it. We use this to determinte the parser
        """
        from virtualizarr import parsers

        try:
            format = self.esm_ds.esmcat.assets.format.value  # type: ignore[union-attr]
        except AttributeError:
            raise ParserInferenceError(
                "Cannot infer parser from intake-esm catalog: "
                "the 'format' field in the catalog's 'assets' specification is missing. "
                "Only asserts with a specified 'format' can be built into an Icechunk store. "
                "See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#assets-object"
            )

        if format is None:
            raise ParserInferenceError(
                "Cannot infer parser from intake-esm catalog: "
                "the 'format' field in the catalog's 'assets' specification is missing."
                "Only asserts with a specified 'format' can be built into an Icechunk store."
                "See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#assets-object"
            )

        PARSER_MAP: dict[str, VirtualizarrParser] = {
            "netcdf": parsers.HDFParser,
            "zarr": parsers.ZarrParser,
            "zarr2": parsers.ZarrParser,
            "zarr3": parsers.ZarrParser,
            "reference": parsers.KerchunkJSONParser,
            # Don't know the best way to handle things that overlap in the format but have different parsers yet.
            # "todo_0": parsers.DMRPPParser,
            # "todo_1": parsers.FITSParser,
            # "todo_2": parsers.NetCDF3Parser,
            # "todo_4": parsers.KerchunkParquetParser,
        }

        try:
            return PARSER_MAP[format]
        except KeyError:
            raise ParserInferenceError(
                f"Unsupported parser format '{format}' specified in intake-esm catalog. "
                f"Supported formats are: {list(PARSER_MAP.keys())}."
            )

    def _create_registry(self) -> ObjectStoreRegistry:
        """
        Create an ObjectStoreRegistry to keep our source files in during the virtuualization
        process. We should be able to infer this from the esm_datastore's assets and our
        `_resolve_storage()` functionlity, determining a root & registry type.

        We also cache the registry on the builder instance.
        """

        path_column = self.esm_ds.esmcat.assets.column_name
        paths = self.esm_ds.esmcat.df[path_column].tolist()
        self.obsstore_registry, self.source_url_prefix = _resolve_store(
            paths, self.store_options
        )

        return self.obsstore_registry

    def _extract_datastore_structure(self) -> tuple[list[str], str]:
        """
        Grab the groupby_attrs and assets column name from the intake-esm catalog,
        which we use to build the icechunk store and populate .zattrs.
        We need these for both the build process and to populate the catalog metadata,
        """
        esmcat = self.esm_ds.esmcat
        agg_control = esmcat.aggregation_control
        groupby_attrs = (
            agg_control.groupby_attrs if agg_control else list(esmcat.df.columns)
        )
        assets_col = esmcat.assets.column_name
        return groupby_attrs, assets_col

    def build(self) -> None:
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

        esmcat = self.esm_ds.esmcat
        groupby_attrs, assets_col = self._extract_datastore_structure()

        # Resolve registry first so self.source_url_prefix is available for the
        # VirtualChunkContainer config below.
        self._create_registry()

        storage = _resolve_storage(self.store_path, self.storage_options)

        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=self.source_url_prefix,
                store=icechunk.local_filesystem_store(
                    self.source_url_prefix.removeprefix("file://")
                ),
            )
        )
        credentials = icechunk.containers_credentials({self.source_url_prefix: None})
        repo = icechunk.Repository.create(storage, config, credentials)

        # Persist the configuration so we don't need to figure it out when we come
        # back to open the store

        repo.save_config()

        # ------------------------------------------------------------------
        # 3. Build each group inside a single transaction
        # ------------------------------------------------------------------
        group_key_map = esmcat._construct_group_keys()

        with repo.transaction(
            "main", message=f"Build Virtual Icechunk catalog for {self.esm_ds.name}"
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

                self.failed_list = []
                try:
                    with open_virtual_mfdataset(
                        urls=file_paths,
                        parser=self.parser,
                        registry=self.obsstore_registry,
                        parallel="dask",
                        decode_times=False,
                        combine="nested",
                        concat_dim="time",
                        compat="override",
                        coords=["time"],
                    ) as vds:
                        vds.vz.to_icechunk(store, group=public_key)

                    # And print a little we're done
                    print(f"Virtualised group {public_key} successfully!")

                    # Write group metadata into .zattrs so the catalog can search
                    # these groups without opening the arrays.
                    zarr_group = zarr.open_group(store, path=public_key, mode="a")
                    zarr_group.attrs.update(group_attrs)
                except Exception as e:
                    self.failed_list.append((public_key, e))

        # Write the JSON sidecar
        store_path_obj = os.path.abspath(self.store_path)
        sidecar_name = os.path.splitext(os.path.basename(store_path_obj))[0]
        sidecar_dir = os.path.dirname(store_path_obj)

        model = VirtualIcechunkCatalogModel(
            store=self.store_path,
            storage_options=self.storage_options,
        )
        model.save(sidecar_name, directory=sidecar_dir or None)
