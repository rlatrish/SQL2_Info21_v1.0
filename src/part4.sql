CREATE DATABASE part4;
\connect part4;
\include part1.sql;
\include part2.sql;
\include part3.sql;


CREATE TABLE ttTableName(v) AS
SELECT * FROM (VALUES('a'),('a'),('a'),('b'),('c'),('c'),('d'),('e')) ttTableName(v);
CREATE TABLE TableNameFirst(v) AS
SELECT * FROM (VALUES('a'),('a'),('a'),('b'),('c'),('c')) TableNameFirst(v);
CREATE TABLE TableNamet(v) AS
SELECT * FROM (VALUES('a'),('a'),('a'),('b')) TableNamet(v);

-- Part 4_1
CREATE OR REPLACE PROCEDURE drop_all_table_name()
AS $$
  DECLARE
    cursor_table CURSOR FOR (
      SELECT tablename
      FROM pg_tables
      WHERE schemaname='public' and tablename LIKE 'tablename%'
    );
    table_name varchar;
  BEGIN
    OPEN cursor_table;
    FETCH cursor_table INTO table_name;
    WHILE FOUND LOOP
      RAISE NOTICE 'Table: % is deleted', table_name;
      EXECUTE 'DROP TABLE IF EXISTS '|| quote_ident(table_name) || ' CASCADE';
      FETCH cursor_table INTO table_name;
    END LOOP;
    CLOSE cursor_table;
  END  
$$ LANGUAGE PLPGSQL;

CALL drop_all_table_name();

-- Part 4_2
CREATE OR REPLACE PROCEDURE all_functions(OUT all_f integer)
AS $$
  DECLARE
    mv RECORD;
    fname VARCHAR = '';
    fparam VARCHAR = '';
  BEGIN
    all_f = 0;
    RAISE NOTICE ' List of names and parameters of all scalar users SQL functions in the current database.';
    FOR mv IN
        SELECT routines.routine_name, parameters.data_type
        FROM information_schema.routines
            LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
        WHERE routines.specific_schema NOT IN ('information_schema', 'pg_catalog') AND
              parameters.ordinal_position IS NOT NULL
        ORDER BY routines.routine_name, parameters.ordinal_position
    LOOP
        IF fname != mv.routine_name
        THEN
            IF fname != ''
            THEN
                RAISE NOTICE '%(%)', fname, fparam;
            END IF;
            all_f = all_f + 1;
            fname = mv.routine_name;
            fparam = mv.data_type;
        ELSE
            fparam = fparam || ', ' || mv.data_type;
        END IF;
    END LOOP;
    IF fname != ''
    THEN
        RAISE NOTICE '%(%)', fname, fparam;
    END IF;
  END
$$ LANGUAGE PLPGSQL;

CALL all_functions(NULL);

-- Part 4_3
CREATE OR REPLACE PROCEDURE drop_triggers(OUT all_t integer)
AS $$
  DECLARE
    cursor_trigger CURSOR FOR (
      SELECT triggers.trigger_name, triggers.event_object_table
      FROM information_schema.triggers
      WHERE triggers.trigger_schema NOT IN ('information_schema', 'pg_catalog') 
      ORDER BY 1
    );
    tr RECORD;
  BEGIN
    all_t = 0;
    OPEN cursor_trigger;
    FETCH cursor_trigger INTO tr;
    WHILE FOUND LOOP
      RAISE NOTICE 'Trigger: % on % is deleted', tr.trigger_name, tr.event_object_table;
      EXECUTE 'DROP TRIGGER '|| quote_ident(tr.trigger_name) || ' ON ' ||
                quote_ident(tr.event_object_table) || ' CASCADE';
      FETCH cursor_trigger INTO tr;
      all_t = all_t + 1;
    END LOOP;
    CLOSE cursor_trigger;
  END  
$$ LANGUAGE PLPGSQL;

CALL drop_triggers(NULL);

-- Part 4_4
CREATE OR REPLACE PROCEDURE find_by_sql_text(IN mask VARCHAR)
AS $$
  DECLARE
    mv RECORD;
  BEGIN
    RAISE NOTICE ' Names and descriptions of object types (only stored procedures and scalar functions) that have the "%".', mask;
    FOR mv IN
        SELECT routines.routine_name, routines.routine_type
        FROM information_schema.routines
        WHERE routines.specific_schema NOT IN ('information_schema', 'pg_catalog') AND
              routines.routine_body = 'SQL' AND
              routines.routine_definition LIKE '%' || mask || '%'
        ORDER BY routines.routine_name
    LOOP
       RAISE NOTICE 'Name: %  , Type: % ', mv.routine_name, mv.routine_type;
    END LOOP;
  END
$$ LANGUAGE PLPGSQL;

CALL find_by_sql_text('all');

