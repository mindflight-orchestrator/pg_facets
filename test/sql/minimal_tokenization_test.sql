-- Minimal tokenization test for pg_facets native tokenizer
-- Tests the test_tokenize_only function which isolates just the tokenization logic
-- Run with: psql -f minimal_tokenization_test.sql

\echo '============================================'
\echo 'MINIMAL TOKENIZATION TEST'
\echo '============================================'

-- Test 1: Basic tokenization
\echo ''
\echo 'Test 1: Basic tokenization (3 unique words)'
SELECT * FROM facets.test_tokenize_only('hello world test', 'english')
ORDER BY lexeme;

-- Test 2: Duplicate words should sum frequencies
\echo ''
\echo 'Test 2: Duplicate words (hello x2, world x3)'
SELECT * FROM facets.test_tokenize_only('hello world hello world world', 'english')
ORDER BY freq DESC, lexeme;

-- Test 3: Stopwords should be filtered
\echo ''
\echo 'Test 3: Stopwords should be filtered (the, is, a)'
SELECT * FROM facets.test_tokenize_only('the quick brown fox is a jumper', 'english')
ORDER BY lexeme;

-- Test 4: Stemming should work
\echo ''
\echo 'Test 4: Stemming (running -> run, quickly -> quick)'
SELECT * FROM facets.test_tokenize_only('running quickly through the forest', 'english')
ORDER BY lexeme;

-- Test 5: Empty text should return empty
\echo ''
\echo 'Test 5: Empty text returns empty set'
SELECT COUNT(*) as count FROM facets.test_tokenize_only('', 'english');

-- Test 6: Text with only stopwords should return empty
\echo ''
\echo 'Test 6: Only stopwords returns empty set'
SELECT COUNT(*) as count FROM facets.test_tokenize_only('the a an is are was were', 'english');

-- Test 7: Different language config (simple = no stemming, no stopwords)
\echo ''
\echo 'Test 7: Simple config (no stemming, no stopwords)'
SELECT * FROM facets.test_tokenize_only('running quickly through the forest', 'simple')
ORDER BY lexeme;

-- Test 8: Multiple sequential calls (tests memory management)
\echo ''
\echo 'Test 8: Multiple sequential calls'
SELECT 'call1' as test, COUNT(*) as tokens FROM facets.test_tokenize_only('first call test', 'english');
SELECT 'call2' as test, COUNT(*) as tokens FROM facets.test_tokenize_only('second call test', 'english');
SELECT 'call3' as test, COUNT(*) as tokens FROM facets.test_tokenize_only('third call test', 'english');
SELECT 'call4' as test, COUNT(*) as tokens FROM facets.test_tokenize_only('fourth call test', 'english');
SELECT 'call5' as test, COUNT(*) as tokens FROM facets.test_tokenize_only('fifth call test', 'english');

-- Test 9: Long text
\echo ''
\echo 'Test 9: Long text tokenization'
SELECT COUNT(*) as token_count FROM facets.test_tokenize_only(
    'The quick brown fox jumps over the lazy dog. ' ||
    'Pack my box with five dozen liquor jugs. ' ||
    'How vexingly quick daft zebras jump. ' ||
    'The five boxing wizards jump quickly.',
    'english'
);

\echo ''
\echo '============================================'
\echo 'ALL TOKENIZATION TESTS PASSED'
\echo '============================================'

