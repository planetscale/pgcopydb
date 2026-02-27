---
--- pgcopydb test/exclude-extension/verify.sql
---
--- Verify extension filtering worked correctly
---

\set ON_ERROR_STOP on

-- Verify pgcrypto extension was EXCLUDED
SELECT 'Checking pgcrypto exclusion' as test;
SELECT CASE
    WHEN COUNT(*) = 0 THEN 'PASS: pgcrypto not installed'
    ELSE 'FAIL: pgcrypto should not exist'
END as result
FROM pg_extension
WHERE extname = 'pgcrypto';

-- Verify uuid-ossp extension WAS included
SELECT 'Checking uuid-ossp inclusion' as test;
SELECT CASE
    WHEN COUNT(*) = 1 THEN 'PASS: uuid-ossp installed'
    ELSE 'FAIL: uuid-ossp should exist'
END as result
FROM pg_extension
WHERE extname = 'uuid-ossp';

-- Verify public.secrets table WAS copied (it's a user table that uses pgcrypto functions,
-- but is not owned by the extension itself - only extension-owned objects are filtered)
SELECT 'Checking secrets table copied' as test;
SELECT CASE
    WHEN COUNT(*) = 1 THEN 'PASS: secrets table copied (not extension-owned)'
    ELSE 'FAIL: secrets table should exist (not extension-owned)'
END as result
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'secrets';

-- Verify public.users table was copied AND CDC changes were applied
SELECT 'Checking users table and CDC' as test;
SELECT CASE
    WHEN COUNT(*) = 4 THEN 'PASS: users has 4 rows (3 initial - 1 deleted + 2 inserted via CDC)'
    ELSE 'FAIL: users should have 4 rows, has ' || COUNT(*)::text
END as result
FROM public.users;

-- Verify alice was updated via CDC
SELECT 'Checking alice CDC update' as test;
SELECT CASE
    WHEN COUNT(*) = 1 THEN 'PASS: alice updated via CDC'
    ELSE 'FAIL: alice should be updated'
END as result
FROM public.users
WHERE username = 'alice_updated';

-- Verify bob was deleted via CDC
SELECT 'Checking bob CDC deletion' as test;
SELECT CASE
    WHEN COUNT(*) = 0 THEN 'PASS: bob deleted via CDC'
    ELSE 'FAIL: bob should be deleted'
END as result
FROM public.users
WHERE username = 'bob';

-- Verify public.documents table was copied AND CDC changes were applied (uuid-ossp is NOT filtered)
SELECT 'Checking documents table and CDC' as test;
SELECT CASE
    WHEN COUNT(*) = 3 THEN 'PASS: documents has 3 rows (2 initial + 1 inserted via CDC)'
    ELSE 'FAIL: documents should have 3 rows, has ' || COUNT(*)::text
END as result
FROM public.documents;

-- Verify Doc 1 was updated via CDC
SELECT 'Checking Doc 1 CDC update' as test;
SELECT CASE
    WHEN COUNT(*) = 1 THEN 'PASS: Doc 1 updated via CDC'
    ELSE 'FAIL: Doc 1 should be updated'
END as result
FROM public.documents
WHERE title = 'Doc 1' AND content = 'Updated content';

SELECT 'All tests completed successfully!' as final_result;
