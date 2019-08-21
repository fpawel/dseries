PRAGMA foreign_keys = ON;
PRAGMA encoding = 'UTF-8';

CREATE TABLE IF NOT EXISTS bucket
(
    bucket_id  INTEGER   NOT NULL PRIMARY KEY,
    name       TEXT      NOT NULL CHECK ( typeof(name) = 'text' AND length(name) > 0 ),
    created_at TIMESTAMP NOT NULL UNIQUE DEFAULT (datetime('now')),
    updated_at TIMESTAMP NOT NULL        DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS series
(
    bucket_id INTEGER NOT NULL,
    addr      INTEGER NOT NULL CHECK (addr > 0),
    var       INTEGER NOT NULL CHECK (var >= 0),
    stored_at REAL    NOT NULL,
    value     REAL    NOT NULL,
    FOREIGN KEY (bucket_id) REFERENCES bucket (bucket_id)
        ON DELETE CASCADE
);

CREATE TRIGGER IF NOT EXISTS trigger_bucket_updated_at
    AFTER INSERT
    ON series
    FOR EACH ROW
BEGIN
    UPDATE bucket
    SET updated_at = datetime('now')
    WHERE bucket.bucket_id = new.bucket_id;
END;

DROP VIEW IF EXISTS bucket_time;
CREATE VIEW IF NOT EXISTS bucket_time AS
    WITH last_bucket AS (
        SELECT bucket_id
        FROM bucket
        ORDER BY created_at DESC
        LIMIT 1)
    SELECT *,
           cast(strftime('%Y', created_at, '+3 hours') AS INTEGER) AS year,
           cast(strftime('%m', created_at, '+3 hours') AS INTEGER) AS month,
           cast(strftime('%d', created_at, '+3 hours') AS INTEGER) AS day,
           cast(strftime('%H', created_at, '+3 hours') AS INTEGER) AS hour,
           cast(strftime('%M', created_at, '+3 hours') AS INTEGER) AS minute,
           bucket_id = (SELECT bucket_id FROM last_bucket)         AS is_last
    FROM bucket;

CREATE VIEW IF NOT EXISTS last_bucket AS
SELECT *
FROM bucket_time
ORDER BY created_at DESC
LIMIT 1;

CREATE VIEW IF NOT EXISTS series_time1 AS
SELECT *,
       cast(strftime('%Y', stored_at) AS INTEGER) AS year,
       cast(strftime('%m', stored_at) AS INTEGER) AS month,
       cast(strftime('%d', stored_at) AS INTEGER) AS day,
       cast(strftime('%H', stored_at) AS INTEGER) AS hour,
       cast(strftime('%M', stored_at) AS INTEGER) AS minute,
       cast(strftime('%f', stored_at) AS REAL)    AS second_real
FROM series;

CREATE VIEW IF NOT EXISTS series_time2 AS
SELECT *, cast(second_real AS INTEGER) AS second
FROM series_time1;

CREATE VIEW IF NOT EXISTS series_time AS
SELECT *,
       cast((second_real - second) * 1000 AS INTEGER) AS millisecond
FROM series_time2;




--SELECT datetime((julianday(current_timestamp)));
--SELECT (julianday(current_timestamp));
--SELECT datetime(2458402.786550926);
--SELECT julianday('now') - julianday('1776-07-04');

