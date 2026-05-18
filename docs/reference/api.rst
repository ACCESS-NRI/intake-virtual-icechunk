.. _api:

API reference
=============

``intake_virtual_icechunk`` provides a few core public components:

- ``intake_virtual_icechunk.core.IcechunkCatalog`` — the main intake catalog
  implementation, registered as the ``virtual_icechunk`` driver.
- ``intake_virtual_icechunk._source.IcechunkDataSource`` — the per-entry data source
  returned when you index into an ``IcechunkCatalog``.
- ``intake_virtual_icechunk.cat.VirtualIcechunkCatalogModel`` — the JSON sidecar model
  used to persist catalog metadata and reopen a store later.
- ``intake_virtual_icechunk.source._build.IcechunkStoreBuilder`` — builds a virtual
  Icechunk store from a pre-built intake-esm catalog.
- ``intake_virtual_icechunk.source._containers.VirtualChunkContainerModel`` — stores
  enough virtual chunk container configuration to round-trip a catalog safely.

The following API summary is auto-generated.

.. autoclass:: intake_virtual_icechunk.core.IcechunkCatalog
   :members:
   :noindex:
   :special-members: __init__

.. autoclass:: intake_virtual_icechunk._source.IcechunkDataSource
   :members:
   :noindex:
   :special-members: __init__

.. autoclass:: intake_virtual_icechunk.cat.VirtualIcechunkCatalogModel
   :members:
   :noindex:
   :special-members: __init__

.. autoclass:: intake_virtual_icechunk.source._build.IcechunkStoreBuilder
   :members:
   :noindex:
   :special-members: __init__

.. autoclass:: intake_virtual_icechunk.source._containers.VirtualChunkContainerModel
   :members:
   :noindex:
   :special-members: __init__
