# TODO / known issues

Lurking issues spotted during a read-through of `src/` and `tests/`. The two
fixed ones (falsy metadata values, `xarray_kwargs` list length) are already
done and removed from this list.

## Bugs / correctness

- [ ] **`_resolve_store` has unreachable code and a latent `NameError`**
      (`src/intake_virtual_icechunk/utils.py`, ~line 142–164).
      The GCS/Azure branches `raise NotImplementedError` *before* their
      `store = ...` assignment (dead code), and the function ends with
      `return ObjectStoreRegistry({f"{bucket}": store})` followed by an
      unreachable `raise ObjectStoreError(...)`. For an unknown scheme nothing
      binds `store`, so it falls through to that `return` and raises
      `NameError: store` instead of the intended `ObjectStoreError`.
      Fix: drop the dead assignments and make the fall-through raise
      `ObjectStoreError` (move it before the bare `return`, or restructure).

- [ ] **Documented `require_all_on` search parameter does nothing**
      (`src/intake_virtual_icechunk/core.py`, `IcechunkCatalog.search`).
      `search(self, **query)` documents `require_all_on`, but since the
      signature is just `**query`, passing it just filters on a (nonexistent)
      column named `require_all_on`. The real implementation,
      `search_apply_require_all_on` in `_search.py`, is only ever called from
      tests — dead in production. Either wire it up properly or drop it from the
      docstring. (The docstring itself notes uncertainty about keeping it.)

## Performance

- [ ] **`df` is recomputed and every dataset re-opened on searched sub-catalogs**
      (`src/intake_virtual_icechunk/core.py`, `df` cached_property).
      `search()` → `_from_parent` shares the open store, but the child's `df`
      re-iterates `self.keys()` and re-opens each allowed group via a fresh
      `IcechunkDataSource`, even though `parent.df` already holds those rows.
      Could slice `parent.df` by `_allowed_keys` instead.

## Vestigial / dead code (low priority, several are intentional placeholders)

- [ ] `_match_query` (`core.py`) is defined but never called — `search` uses
      `pl_search` instead.
- [ ] `pl_search` iterable-pattern branch (`_search.py`, ~line 120–137) is
      `raise NotImplementedError` followed by unreachable matching code (a
      placeholder for future pattern-in-iterable support).
- [ ] `pl_search` no-op cast (`_search.py`, ~line 75–79) casts every column to
      its own existing dtype — appears to do nothing.
- [ ] `to_dask` may double-warn on Python < 3.13 (`core.py`) — both the
      `typing_extensions.deprecated` decorator and the manual `warnings.warn`
      fire.

## Robustness (nice-to-have)

- [ ] `pl_search` non-iterable pattern path (`_search.py`, ~line 105–110):
      `"|".join(subquery)` will `TypeError` if a `None` (from the `pd.isna`
      substitution above) survives into a pattern query, and `.str.contains`
      assumes the column is string-typed.
- [ ] Builders catch per-entry exceptions and still write the sidecar, so a
      partially-built store looks complete (failures land in `self.failed_list`
      but `build()` emits no end-of-run summary that some groups failed).
