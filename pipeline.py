"""A runnable example of incremental recipe catalog ingestion."""

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory


def initialize_target(target):
    target.executescript("""
        CREATE TABLE checkpoint (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            completed_through INTEGER NOT NULL,
            window_end INTEGER,
            page_timestamp INTEGER NOT NULL,
            page_id INTEGER NOT NULL
        );
        INSERT INTO checkpoint VALUES (1, 0, NULL, 0, 0);

        CREATE TABLE mirror (
            recipe_id INTEGER PRIMARY KEY,
            payload TEXT NOT NULL,
            content_hash TEXT NOT NULL
        );
        CREATE TABLE refresh_outbox (
            event_id INTEGER PRIMARY KEY,
            recipe_id INTEGER NOT NULL,
            content_hash TEXT NOT NULL
        );
    """)


def sync_window(source, target, *, source_now, page_size=2, overlap=5):
    """Sync one closed time window for a single worker."""
    if page_size < 1 or overlap < 0:
        raise ValueError("page size must be positive and overlap must be nonnegative")

    completed, upper, timestamp, recipe_id = target.execute("""
        SELECT completed_through, window_end, page_timestamp, page_id
        FROM checkpoint WHERE id = 1
    """).fetchone()
    if upper is None:
        if source_now < completed:
            raise ValueError("source clock is behind the completed watermark")
        upper = source_now
        timestamp, recipe_id = max(0, completed - overlap), 0
        with target:
            target.execute("""
                UPDATE checkpoint SET window_end = ?, page_timestamp = ?, page_id = 0
                WHERE id = 1
            """, (upper, timestamp))

    while True:
        page = source.execute("""
            SELECT id, title, servings, updated_at
            FROM source_recipes
            WHERE (updated_at, id) > (?, ?) AND updated_at <= ?
            ORDER BY updated_at, id
            LIMIT ?
        """, (timestamp, recipe_id, upper, page_size)).fetchall()

        with target:
            if not page:
                target.execute("""
                    UPDATE checkpoint
                    SET completed_through = ?, window_end = NULL,
                        page_timestamp = 0, page_id = 0
                    WHERE id = 1
                """, (upper,))
                return

            for recipe_id, title, servings, timestamp in page:
                payload = json.dumps({"title": title.strip(), "servings": servings},
                                     sort_keys=True, separators=(",", ":"))
                digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                change = target.execute("""
                    INSERT INTO mirror (recipe_id, payload, content_hash) VALUES (?, ?, ?)
                    ON CONFLICT (recipe_id) DO UPDATE
                    SET payload = excluded.payload, content_hash = excluded.content_hash
                    WHERE mirror.content_hash <> excluded.content_hash
                """, (recipe_id, payload, digest))
                if change.rowcount:
                    target.execute("""
                        INSERT INTO refresh_outbox (recipe_id, content_hash)
                        VALUES (?, ?)
                    """, (recipe_id, digest))

            # Commit mirrored rows, refresh events, and cursor as one unit.
            target.execute("""
                UPDATE checkpoint SET page_timestamp = ?, page_id = ? WHERE id = 1
            """, (timestamp, recipe_id))


def snapshot(target):
    """Read recipe IDs, the number of refresh events, and the durable cursor."""
    recipe_ids = [row[0] for row in target.execute("SELECT recipe_id FROM mirror ORDER BY recipe_id")]
    event_count = target.execute("SELECT count(*) FROM refresh_outbox").fetchone()[0]
    checkpoint = target.execute("""
        SELECT completed_through, window_end, page_timestamp, page_id
        FROM checkpoint WHERE id = 1
    """).fetchone()
    return recipe_ids, event_count, checkpoint


def main():
    with TemporaryDirectory() as tmp, closing(sqlite3.connect(":memory:")) as source:
        source.executescript("""
            CREATE TABLE source_recipes (
                id INTEGER PRIMARY KEY CHECK (id > 0),
                title TEXT NOT NULL,
                servings INTEGER NOT NULL CHECK (servings > 0),
                updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
            );
            CREATE INDEX source_recipes_cursor ON source_recipes (updated_at, id);
            INSERT INTO source_recipes VALUES
                (1, 'Tomato soup', 2, 100),
                (2, 'Lentil stew', 4, 100),
                (3, 'Ginger rice', 3, 100),
                (4, 'Citrus salad', 2, 101);
        """)
        target_path = Path(tmp) / "catalog.sqlite"
        with closing(sqlite3.connect(target_path)) as target:
            initialize_target(target)
            # Fail the second page after its rows and refresh events have been written.
            target.executescript("""
                CREATE TEMP TRIGGER fail_second_page
                BEFORE UPDATE ON checkpoint WHEN NEW.page_id = 4
                BEGIN
                    SELECT RAISE(ABORT, 'simulated checkpoint failure');
                END;
            """)
            try:
                sync_window(source, target, source_now=105)
            except sqlite3.IntegrityError as exc:
                if str(exc) != "simulated checkpoint failure":
                    raise
            else:
                raise AssertionError("expected the second page to fail")
            assert snapshot(target) == ([1, 2], 2, (0, 105, 100, 2))
            print("After failure: 2 recipes and 2 refresh events; first-page cursor preserved.")

        with source:
            source.execute("INSERT INTO source_recipes VALUES (5, 'Bean tacos', 2, 106)")
        with closing(sqlite3.connect(target_path)) as target:
            sync_window(source, target, source_now=110)
            assert snapshot(target) == ([1, 2, 3, 4], 4, (105, None, 0, 0))
            print("After restart: 4 recipes and 4 refresh events; original window completed.")

            with source:
                # This update arrives after the completed watermark has passed its timestamp.
                source.execute("UPDATE source_recipes SET servings = 6, updated_at = 104 WHERE id = 2")
            sync_window(source, target, source_now=110)
            assert snapshot(target) == ([1, 2, 3, 4, 5], 6, (110, None, 0, 0))
            stored = target.execute("SELECT payload FROM mirror WHERE recipe_id = 2").fetchone()[0]
            assert json.loads(stored)["servings"] == 6

            sync_window(source, target, source_now=110)
            assert snapshot(target) == ([1, 2, 3, 4, 5], 6, (110, None, 0, 0))
            print("After overlapping replay: 5 recipes and 6 refresh events; replay added none.")


if __name__ == "__main__":
    main()
