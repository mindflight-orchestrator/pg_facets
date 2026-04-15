-- Regression test for generic-plan array detoast bug.
-- PL/pgSQL switches from custom to generic plan after 5 executions.
-- Before the fix, the 6th call to search_documents_native crashed with
-- "cache lookup failed for type <garbage>" because the filters array
-- was accessed via DatumGetPointer (raw cast) instead of pg_detoast_datum,
-- which is required to flatten expanded varlena representations used by
-- generic plans.

\echo '=============================================='
\echo 'Generic Plan Detoast Regression Test'
\echo '=============================================='

DROP SCHEMA IF EXISTS gpd_test CASCADE;
CREATE SCHEMA gpd_test;

CREATE TABLE gpd_test.items (
    id SERIAL PRIMARY KEY,
    status TEXT NOT NULL,
    region TEXT NOT NULL
);

INSERT INTO gpd_test.items (status, region)
SELECT
    CASE (i % 3)
        WHEN 0 THEN 'open'
        WHEN 1 THEN 'closed'
        ELSE 'pending'
    END,
    CASE (i % 2)
        WHEN 0 THEN 'north'
        ELSE 'south'
    END
FROM generate_series(1, 20) AS s(i);

SELECT facets.add_faceting_to_table(
    'gpd_test.items',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('status'),
        facets.plain_facet('region')
    ],
    populate => true
);

-- 30 iterations forces PL/pgSQL well past the 5-call generic plan threshold.
DO $$
DECLARE
    i INT;
    cnt BIGINT;
BEGIN
    FOR i IN 1..30 LOOP
        SELECT COUNT(*) INTO cnt
        FROM facets.filter_documents_by_facets(
            'gpd_test',
            jsonb_build_object('status', 'closed', 'region', 'south'),
            'items'
        );
    END LOOP;
    RAISE NOTICE 'generic_plan_detoast: 30 iterations OK (last count=%)', cnt;
END;
$$;

-- Also test with varying filter values (different JSONB each time).
DO $$
DECLARE
    i INT;
    cnt BIGINT;
    statuses TEXT[] := ARRAY['open', 'closed', 'pending'];
    regions  TEXT[] := ARRAY['north', 'south'];
BEGIN
    FOR i IN 1..30 LOOP
        SELECT COUNT(*) INTO cnt
        FROM facets.filter_documents_by_facets(
            'gpd_test',
            jsonb_build_object(
                'status', statuses[1 + (i % 3)],
                'region', regions[1 + (i % 2)]
            ),
            'items'
        );
    END LOOP;
    RAISE NOTICE 'generic_plan_detoast (varying filters): 30 iterations OK';
END;
$$;

-- Single-filter variant: ensures the fix works regardless of filter count.
DO $$
DECLARE
    i INT;
    cnt BIGINT;
BEGIN
    FOR i IN 1..15 LOOP
        SELECT COUNT(*) INTO cnt
        FROM facets.filter_documents_by_facets(
            'gpd_test',
            jsonb_build_object('status', 'open'),
            'items'
        );
    END LOOP;
    RAISE NOTICE 'generic_plan_detoast (single filter): 15 iterations OK';
END;
$$;

DROP SCHEMA gpd_test CASCADE;

\echo 'Generic plan detoast regression test passed'
