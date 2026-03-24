-- Create extensions in the expected order
CREATE EXTENSION IF NOT EXISTS pgcrypto CASCADE;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- pg_cron requires special postgresql.conf configuration, skip for tests
-- CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_facets;

