---
--- pgcopydb test/exclude-extension/dml.sql
---
--- CDC changes to test extension filtering during replay
---

-- CDC changes to public.users (should be applied)
INSERT INTO public.users (username) VALUES ('dave'), ('eve');

UPDATE public.users SET username = 'alice_updated' WHERE username = 'alice';

DELETE FROM public.users WHERE username = 'bob';

-- CDC changes to public.documents with uuid-ossp (should be applied, uuid-ossp is NOT filtered)
INSERT INTO public.documents (title, content)
VALUES ('Doc 3', 'Content 3');

UPDATE public.documents SET content = 'Updated content' WHERE title = 'Doc 1';

-- CDC changes to public.secrets using pgcrypto (should be FILTERED OUT, pgcrypto IS filtered)
INSERT INTO public.secrets (secret_data, secret_hash)
VALUES ('newsecret', crypt('newsecret', gen_salt('bf')));

UPDATE public.secrets
SET secret_hash = crypt('updated_password', gen_salt('bf'))
WHERE secret_data = 'password123';

DELETE FROM public.secrets WHERE id = 1;
