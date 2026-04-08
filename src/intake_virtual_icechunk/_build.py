# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


class IcechunkStoreBuilder:
    """Build a virtual Icechunk store from an existing intake-esm datastore.

    Given a pre-built intake-esm catalog, this builder iterates over every
    dataset in the catalog, opens each one with VirtualiZarr to create virtual
    references, and writes each dataset as a named Zarr *group* inside a single
    Icechunk store.  The result is one Icechunk store with one group per
    dataset, mirroring the logical structure of the intake-esm catalog.

    Icechunk session, store, and branch management is handled internally so
    callers need only supply paths.

    Parameters
    ----------
    catalog_path : str
        Path to an existing intake-esm catalog JSON file.
    store_path : str
        Path or URI at which to create (or open) the Icechunk store.
    storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend.
    """

    def __init__(self, catalog_path, store_path, storage_options=None):
        self.catalog_path = catalog_path
        self.store_path = store_path
        self.storage_options = storage_options or {}

    def build(self):
        """Build the Icechunk store.

        For each dataset in the intake-esm catalog:

        1. Opens the dataset via intake-esm.
        2. Creates virtual references using VirtualiZarr.
        3. Writes the dataset to a named Zarr group in the Icechunk store.

        The group name for each dataset is derived from the catalog's
        ``groupby_attrs``, so the Icechunk store structure mirrors the
        intake-esm grouping.  Icechunk sessions, stores, and branches are
        managed internally.
        """
        raise NotImplementedError
