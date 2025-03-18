-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
DROP PROCEDURE IF EXISTS proc;
DROP INDEX IF EXISTS i0;
CREATE TABLE t (a INT PRIMARY KEY, b STRING, c STRING);
CREATE PROCEDURE proc(int, text, text) AS $$INSERT INTO t VALUES($1, $2, $3)$$;
CREATE INDEX i0 ON t (a);

-- TEST: select-ok
-- SQL:
SELECT * FROM public._pico_table;
SELECT public._pico_table.id FROM _pico_table;
SELECT public._pico_table.id FROM public._pico_table;

-- TEST: create-table-error
-- SQL:
CREATE TABLE public.t (a INT PRIMARY KEY, b STRING, c STRING);
CREATE TABLE "public".t (a INT PRIMARY KEY, b STRING, c STRING);
-- ERROR:
table t already exists
table t already exists

-- TEST: create-partition-unsupported-error
-- SQL:
CREATE TABLE t_1_a PARTITION OF public.t FOR VALUES IN (1, 2, 3);
-- ERROR:
rule PARTITION OF logic is not supported yet. not implemented

-- TEST: create-procedure-error
-- SQL:
CREATE PROCEDURE public.proc(int, text, text) AS $$INSERT INTO t VALUES($1, $2, $3)$$;
CREATE PROCEDURE "public".proc(int, text, text) AS $$INSERT INTO t VALUES($1, $2, $3)$$;
-- ERROR:
procedure proc already exists
procedure proc already exists

-- TEST: alter-and-call-procedure-ok
-- SQL:
CALL public.proc(-1, '1', '2');
ALTER PROCEDURE public.proc RENAME TO proc2;
CALL public.proc2(-2, '1', '2');
DROP PROCEDURE public.proc2;

-- TEST: call-dropped-procedure-error
-- SQL:
CALL public.proc(-1, '1', '2');
-- ERROR:
invalid routine: routine proc not found

-- TEST: create-index-error
-- SQL:
CREATE INDEX public.i0 ON public.t (a);
-- ERROR:
rule parsing error

-- TEST: drop-index-ok
-- SQL:
DROP INDEX public.i0;

-- TEST: alter-table-unsupported-error
-- SQL:
ALTER TABLE public.t ADD COLUMN d INT;
-- ERROR:
ALTER TABLE is reserved for future use

-- TEST: dml-ok
-- SQL:
INSERT INTO public.t VALUES(1,'2','3');
UPDATE public.t SET b='kek' WHERE a < 5;
DELETE FROM public.t WHERE a < 3;
TRUNCATE TABLE public.t;
DROP TABLE public.t
