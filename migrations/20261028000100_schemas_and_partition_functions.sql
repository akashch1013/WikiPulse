-- migrate:up
CREATE SCHEMA ops;
CREATE SCHEMA stream;
CREATE SCHEMA enrich;
CREATE SCHEMA mart;

GRANT USAGE ON SCHEMA ops, stream, mart TO wikipulse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO wikipulse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA stream GRANT SELECT ON TABLES TO wikipulse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO wikipulse_reader;

-- Monthly range partitions with UTC bounds, named <table>_pYYYYMM. There is no DEFAULT partition, so a write
-- outside the created months fails; writers call this before loading a period.
CREATE FUNCTION ops.ensure_month_partitions(parent regclass, from_month date, to_month date)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    parent_schema text;
    parent_name text;
    month_start date := date_trunc('month', from_month)::date;
    created integer := 0;
    partition_name text;
BEGIN
    SELECT n.nspname, c.relname INTO parent_schema, parent_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = parent;

    WHILE month_start <= date_trunc('month', to_month)::date LOOP
        partition_name := format('%s_p%s', parent_name, to_char(month_start, 'YYYYMM'));
        IF to_regclass(format('%I.%I', parent_schema, partition_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                parent_schema, partition_name, parent,
                month_start::timestamp AT TIME ZONE 'UTC',
                (month_start + interval '1 month')::timestamp AT TIME ZONE 'UTC'
            );
            created := created + 1;
        END IF;
        month_start := (month_start + interval '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

-- Daily range partitions with UTC bounds, named <table>_pYYYYMMDD. Used by the short-retention stream tables.
CREATE FUNCTION ops.ensure_day_partitions(parent regclass, from_day date, to_day date)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    parent_schema text;
    parent_name text;
    day_start date := from_day;
    created integer := 0;
    partition_name text;
BEGIN
    SELECT n.nspname, c.relname INTO parent_schema, parent_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = parent;

    WHILE day_start <= to_day LOOP
        partition_name := format('%s_p%s', parent_name, to_char(day_start, 'YYYYMMDD'));
        IF to_regclass(format('%I.%I', parent_schema, partition_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                parent_schema, partition_name, parent,
                day_start::timestamp AT TIME ZONE 'UTC',
                (day_start + 1)::timestamp AT TIME ZONE 'UTC'
            );
            created := created + 1;
        END IF;
        day_start := day_start + 1;
    END LOOP;
    RETURN created;
END;
$$;

-- Drops partitions whose upper bound is at or before the cutoff. Works for any range partitioning on
-- timestamptz, so retention granularity is the partition size.
CREATE FUNCTION ops.drop_partitions_before(parent regclass, cutoff timestamptz)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    part record;
    dropped integer := 0;
BEGIN
    FOR part IN
        SELECT c.oid::regclass AS child,
               (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \(''([^'']+)''\)'))[1]::timestamptz AS upper_bound
        FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = parent
    LOOP
        IF part.upper_bound <= cutoff THEN
            EXECUTE format('DROP TABLE %s', part.child);
            dropped := dropped + 1;
        END IF;
    END LOOP;
    RETURN dropped;
END;
$$;

CREATE FUNCTION ops.drop_day_partitions_before(parent regclass, cutoff date)
RETURNS integer
LANGUAGE sql
AS $$
    SELECT ops.drop_partitions_before(parent, cutoff::timestamp AT TIME ZONE 'UTC');
$$;

-- migrate:down
DROP FUNCTION ops.drop_day_partitions_before(regclass, date);
DROP FUNCTION ops.drop_partitions_before(regclass, timestamptz);
DROP FUNCTION ops.ensure_day_partitions(regclass, date, date);
DROP FUNCTION ops.ensure_month_partitions(regclass, date, date);
ALTER DEFAULT PRIVILEGES IN SCHEMA mart REVOKE SELECT ON TABLES FROM wikipulse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA stream REVOKE SELECT ON TABLES FROM wikipulse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops REVOKE SELECT ON TABLES FROM wikipulse_reader;
REVOKE USAGE ON SCHEMA ops, stream, mart FROM wikipulse_reader;
DROP SCHEMA mart;
DROP SCHEMA enrich;
DROP SCHEMA stream;
DROP SCHEMA ops;
