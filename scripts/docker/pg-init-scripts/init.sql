CREATE TABLE locks (
    name TEXT PRIMARY KEY,
    leaderID TEXT NOT NULL
);

CREATE TABLE kv (
    key TEXT PRIMARY KEY,
    value BYTEA
);

CREATE OR REPLACE FUNCTION notify_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM cron.schedule('delete_' || NEW.name, '6 seconds', FORMAT('DELETE FROM locks WHERE name = %L', NEW.name));
        PERFORM pg_notify('lock_change', 'INSERT:' || NEW.leaderID::text);
    END IF;

    IF TG_OP = 'DELETE' THEN
        PERFORM cron.unschedule('delete_' || OLD.name);
        PERFORM pg_notify('lock_change', 'DELETE:' || OLD.leaderID::text);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER lock_change_trigger
AFTER INSERT OR DELETE ON locks
FOR EACH ROW EXECUTE FUNCTION notify_changes();

CREATE EXTENSION IF NOT EXISTS pg_cron;