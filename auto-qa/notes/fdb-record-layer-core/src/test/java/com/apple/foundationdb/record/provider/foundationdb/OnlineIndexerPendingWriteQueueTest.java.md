# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerPendingWriteQueueTest.java
## Visits
- 2026-11-19 bf040b1e3
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/queue/PendingWritesQueue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingPendingWriteQueue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingBase.java (markIndexReadableForIndex, drainIndexes)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingThrottle.java (populateDrainRequiredIndexes)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainerWithQueue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.java (WRITE_ONLY_WITH_QUEUE update / deleteWhere enqueue)
## Explored
- PendingWritesQueue.getQueueCursor with skip: CANDIDATE BUG, skip ignored (inner cursor clears skip, unsplitter only applies row limit). Same as Lucene AQA-12.
- getQueueCursor with returned row limit + continuation, paging 2 at a time: OK.
- Replay order for the same record (update then delete in separate txns; two updates in one txn; delete+reinsert then update), both in built and unbuilt ranges: OK.
- Split queue entries (record > 100KB, split long records): drained, applied and cleared, counter cleared: OK.
## Rejected
- setPendingWriteQueueIndexesMaxDrainAttempts(0 / negative): javadoc says "Must be positive" but does not promise validation; a 0 still runs one attempt. Not worth reporting.
## Untried
- getQueueCursor with skip combined with a row limit and a continuation (does the continuation after skip point to the right place once skip is fixed).
- getQueueCursor reverse scan (ScanProperties.REVERSE_SCAN): the unsplitter is told reverse, check order.
- Byte/time scan limits on getQueueCursor: the inner cursor clears row/time limits, check that byte limits stop at entry boundaries and the continuation resumes correctly with split entries.
- Queue with entries from two incarnations (store.updateIncarnation between enqueues): drain order should be by incarnation first.
- DELETE_WHERE queued followed by a save of a record in the deleted prefix (order of replay).
- Drain when MAX_PENDING_WRITE_QUEUE_SIZE is lowered below the current size (drain must still work; enqueue during drain).
