# Contributing to pg_facets

Thanks for contributing to `pg_facets`.

## Commit Sign-off (Required)

All commits must include a Developer Certificate of Origin (DCO) sign-off:

```bash
git commit -s -m "feat(scope): your message"
```

This adds a `Signed-off-by:` line and confirms you have the right to submit
the contribution under this repository's license.

## Contribution Provenance (Required)

By submitting a contribution, you confirm that:

- You wrote the code yourself, or you have explicit rights to contribute it.
- The contribution does not include confidential, proprietary, or NDA-restricted material.
- The contribution does not copy code from private employer/client repositories
  without explicit written authorization.

## Dependency Note

This repository depends on `pg_roaringbitmap` (Apache-2.0). While an upstream
bug remains unresolved, this project may carry minimal temporary patches to
that dependency. Keep such patches documented and easy to remove when upstream
is fixed.

