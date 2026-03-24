# Security audit: pg_facets

## Scope

| Field | Value |
|-------|--------|
| Extension | `pg_facets` |
| Control file | `pg_facets.control` |
| `default_version` (snapshot) | `0.4.3` |
| `module_pathname` | `$libdir/pg_facets` |
| `requires` | `roaringbitmap` |
| `superuser` | **not set** |
| `relocatable` | `true` |

**Purpose (short):** Faceting, BM25-style search, Roaring Bitmap indexes, and related SQL helpers under schema `facets`.

## Artifact inventory (first-party)

- `pg_facets.control`
- `sql/pg_facets--*.sql` (multiple version and upgrade scripts; latest full script audited: `pg_facets--0.4.3.sql`)
- `src/`, build files  
- `deps/pg_roaringbitmap/` — third-party; separate audit if shipped.

## Checklist — code

| Item | Result | Evidence / notes |
|------|--------|------------------|
| `SECURITY DEFINER` + `search_path` | **Pass** | No `SECURITY DEFINER` in `pg_facets--0.4.3.sql` (`rg 'SECURITY DEFINER'` → none) |
| `CREATE OR REPLACE FUNCTION` | **Review** | Extensive `CREATE OR REPLACE FUNCTION facets.*` (and dynamic trigger/function generation around lines 1140+). Expected for upgrades; verify upgrade-only vs fresh install paths per PostgreSQL extension guidelines. |
| `superuser = true` | **Pass** | Absent from `pg_facets.control` |
| `pg_extension_config_dump` | **Review** | Lines 70 and 86 in `pg_facets--0.4.3.sql` show **commented-out** `pg_catalog.pg_extension_config_dump` for `facets.faceted_table` and `facets.facet_definition`. If those tables hold user configuration that must survive `pg_dump`/`pg_restore` in extension mode, consider enabling and validating. |

## Checklist — cluster / deployment

| Item | Result |
|------|--------|
| PostgreSQL logging / pgAudit | **Operator** |
| Extension allowlist | **Operator** |

## Checklist — complementary

| Item | Result | Notes |
|------|--------|-------|
| RLS | **Not in extension SQL** | Evaluate RLS if facet-indexed tables contain tenant-isolated data |
| `pgcrypto` | **N/A** in audited snapshot | — |

## Findings summary (snapshot)

1. **Medium (config dump):** Commented `pg_extension_config_dump` calls — decide explicitly whether to enable for production backup semantics.
2. **Low:** Volume of `CREATE OR REPLACE` — maintain clear upgrade script discipline.

## Re-audit commands

```bash
rg -n "SECURITY DEFINER|CREATE OR REPLACE|pg_extension_config_dump|search_path" sql/pg_facets--0.4.3.sql
cat pg_facets.control
```
