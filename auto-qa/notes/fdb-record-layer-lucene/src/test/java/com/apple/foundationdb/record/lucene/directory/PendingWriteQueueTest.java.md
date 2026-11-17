# Notes: fdb-record-layer-lucene/src/test/java/com/apple/foundationdb/record/lucene/directory/PendingWriteQueueTest.java
## Visits
- 2026-11-16 65f9c37bc
## Production code
- fdb-record-layer-lucene/src/main/java/com/apple/foundationdb/record/lucene/directory/PendingWriteQueue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/SplitHelper.java (KeyValueUnsplitter)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/cursors/RowLimitedCursor.java
## Explored
- replayQueuedOperations with exactly maxEntriesToReplay entries: CANDIDATE BUG, throws TooManyPendingWritesException (limitRowsTo reports RETURN_LIMIT_REACHED after N rows without peeking), field javadoc says fail only when queue has "more than" N.
- replayQueuedOperations with N+1 entries: fails as documented (PASS).
- getQueueCursor with ExecuteProperties skip: CANDIDATE BUG, skip ignored (inner clears skip, KeyValueUnsplitter's CursorLimitManager never applies skip, no .skip() added); javadoc says skip honored at entry level.
- getQueueCursor with scannedBytesLimit=10 over split (250KB) entries + continuations: each entry returned exactly once (PASS).
## Rejected
## Untried
- getQueueCursor with skip + returnedRowLimit + continuation interaction (once skip is fixed).
- getQueueCursor reverse scan (ScanProperties reverse=true) over split entries.
- clearEntry of an entry obtained with allowIncarnation=false queue (key tuple size 1) and counter consistency.
- enqueue with maxQueueSize when counter has drifted negative (e.g. clearEntry on uninitialized counter).
- replayQueuedOperations of INSERT entries with PRIMARY_KEY_SERIALIZATION_FORMAT index option.
