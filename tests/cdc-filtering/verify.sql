-- Verify public schema data was copied AND CDC changes were applied
SELECT 'Checking public.users' as test;
SELECT count(*) as user_count FROM public.users;
-- Should be 5 (3 initial + 2 from CDC)

SELECT 'Checking alice email update' as test;
SELECT email FROM public.users WHERE username = 'alice';
-- Should be 'alice.new@example.com' (updated via CDC)

SELECT 'Checking bob username update' as test;
SELECT username FROM public.users WHERE username = 'robert';
-- Should exist (bob was renamed to robert via CDC)

SELECT 'Checking public.orders' as test;
SELECT count(*) as order_count FROM public.orders;
-- Should be 2 (2 initial - 1 deleted + 1 inserted via CDC)

-- Verify cron schema was EXCLUDED (should not exist on target)
SELECT 'Checking cron schema exclusion' as test;
SELECT count(*) as cron_schema_exists
FROM information_schema.schemata
WHERE schema_name = 'cron';
-- Should be 0 (schema should not exist)

-- Verify excluded_schema was EXCLUDED (should not exist on target)
SELECT 'Checking excluded_schema exclusion' as test;
SELECT count(*) as excluded_schema_exists
FROM information_schema.schemata
WHERE schema_name = 'excluded_schema';
-- Should be 0 (schema should not exist)
