/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

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