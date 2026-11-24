# sample-code-ai

This Python sample demonstrates how a recipe catalog can stay in sync with a
search index when a batch fails or a polling window replays older rows. It
adapts the ingestion stage of a meal-planning pipeline that reads recipes and
ingredient data from MySQL, mirrors records to PostgreSQL, and prepares
documents for semantic search with pgvector.

## Run the sample

Use Python 3.12 and its standard library. The demo uses synthetic recipes and
temporary SQLite databases.

```sh
python3 pipeline.py
```

```text
After failure: 2 recipes and 2 refresh events; first-page cursor preserved.
After restart: 4 recipes and 4 refresh events; original window completed.
After overlapping replay: 5 recipes and 6 refresh events; replay added none.
```

## How it works

1. Pin an upper timestamp for each polling window. Page by `(updated_at, id)`
   so recipes with the same timestamp can span multiple pages.
2. Hash the normalized recipe content. Write the mirror and enqueue a
   downstream refresh only when the content changes.
3. Commit the mirror, refresh events, and checkpoint in one transaction. The
   demo fails after writing a page, reopens the target database, and resumes
   from the last committed cursor.
4. Overlap consecutive windows to catch late updates within the overlap.
   Unchanged rows keep their existing hash and emit no new events.

The sample runs a single worker and stops at the refresh queue. The full
pipeline uses separate workers to build retrieval documents and embeddings.

## Scale and next change

The source inventory estimated about 8,000 recipe rows. In a separate local
benchmark on a 300-row ingredient batch, the PostgreSQL write phase took about
3.0 seconds of the 3.3-second end-to-end run. I would replace per-row database
writes with a set-based upsert that returns changed record IDs, then enqueue
refresh events and advance the checkpoint in the same transaction.
