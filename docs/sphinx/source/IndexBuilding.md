# Building indexes online

When a new index is added to a store that already has data, the Record Layer may mark the index write-only instead of building it in the opening transaction. [Schema evolution](SchemaEvolution.md#adding-an-index-to-a-record-type) covers when that happens. Use [`OnlineIndexer`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.html) to fill the index in many small transactions while the store stays available for ordinary reads and writes.

Because the index is write-only during the build, live record saves keep it up to date. The online build only has to catch up; it does not race a moving target.

## Basic usage

Build across multiple transactions, then mark the index readable:

```java
try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
    indexBuilder.buildIndex();
}
```

If the store is small enough to finish in the current transaction:

```java
try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
    indexBuilder.rebuildIndex(recordStore);
}
```

`rebuildIndex` is the same idea as the automatic build that runs when a store has fewer than about 200 records. For anything larger, use `buildIndex()`.

By default `buildIndex()` continues a write-only index that is already partly built. That is the recommended behavior unless you have reason to think the existing index data is corrupt.

## Limiting impact on live traffic

Index builds run at [`FDBTransactionPriority.BATCH`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/FDBTransactionPriority.html) unless you raise the priority. The builder also caps how much work each transaction does. The knobs that usually matter:

| Setter | Default | What it does |
| --- | --- | --- |
| `setLimit` | 100 records | Maximum records processed in one transaction. Lower it if transactions time out. |
| `setInitialLimit` | same as `setLimit` | Start below the max so the first transactions are cheaper. |
| `setMaxWriteLimitBytes` | 900,000 | Commit when the transaction write size exceeds this. |
| `setTransactionTimeLimitMilliseconds` | 4,000 | Commit and start a new transaction before FoundationDB's 5 second limit. |
| `setRecordsPerSecond` | 10,000 | Throttle the scan so the build does not crowd out user traffic. |
| `setMaxRetries` | 100 | Extra retries for a single range, including `transaction_too_large`. |
| `setProgressLogIntervalMillis` | `-1` (off) | Log scanned ranges. `0` logs after every commit. |

Progress is tracked by default. Scanned-record counts are persisted and can be read through `IndexBuildState`.

`setConfigLoader` can replace these values between transactions if you want the build to react to load.

## Parallel builds

One indexer is enough for a modest store. For a large store, run several processes with the same arguments and turn on mutual indexing:

```java
OnlineIndexer.IndexingPolicy policy = OnlineIndexer.IndexingPolicy.newBuilder()
        .setMutualIndexing()
        .build();

try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
        .setRecordStore(recordStore)
        .setIndex("newIndex")
        .setIndexingPolicy(policy)
        .setRecordsPerSecond(2000)
        .build()) {
    indexBuilder.buildIndex();
}
```

Mutual indexing splits the primary-key space into fragments — by approximate FoundationDB shard boundaries, unless you pass your own — and lets each process claim work without duplicating it. A `RangeSet` on the index records which ranges are done, which also keeps non-idempotent indexes such as `COUNT` consistent.

To choose the fragments yourself, pass primary-key boundaries. The list can start and end with `null` so the full key space is covered:

```java
OnlineIndexer.IndexingPolicy.newBuilder()
        .setMutualIndexing()
        .setMutualIndexingBoundaries(primaryKeyBoundaries)
        .build();
```

Mutual indexing is experimental. Every participant must use the same policy.

## Building from another index

If an existing readable index already covers the records you need, scan that instead of the primary store:

```java
OnlineIndexer.IndexingPolicy.newBuilder()
        .setSourceIndex("existingIndex")
        .build();
```

The source index must be readable, idempotent, and cover every record the new index needs. If it cannot be used, the build falls back to a record scan unless you call `forbidRecordScan()`.

## Partly built indexes

`IndexingPolicy` chooses what to do when the index is disabled, write-only, already readable, or was started with a different method (`CONTINUE`, `REBUILD`, or `ERROR`). The default is to continue a write-only build.

See the [`OnlineIndexer`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.html) javadoc for the full builder API.
