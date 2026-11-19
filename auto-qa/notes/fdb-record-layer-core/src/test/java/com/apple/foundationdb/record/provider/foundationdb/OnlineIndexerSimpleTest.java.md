# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerSimpleTest.java
## Visits
- 2026-11-19 bf040b1e3
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingBase.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingMultiTargetByRecords.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java (IndexingPolicy, markReadable, markReadableIfBuilt)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.java (markIndexReadable / checkAndUpdateBuiltIndexState)
## Explored
- Companion extends OnlineIndexerTest (abstract, no @Test), like the original.
- markReadableIfBuilt on a WRITE_ONLY_WITH_QUEUE index with a non-empty queue: CANDIDATE BUG. It only checks missing ranges and calls
  store.markIndexReadable, which erases the queue (eraseAllIndexingDataButTheLockAndRangeSet) - queued user updates lost, readable index stale.
- OnlineIndexer.markReadable() on a fresh indexer (queue index): CANDIDATE BUG, same root cause. IndexingBase.markIndexReadable drains /
  asserts empty only for common.getQueuedIndexes(), which is filled only by handleStateAndDoBuildIndexAsync (cacheQueuedIndexes).
  Control: buildIndex(true) of a new indexer drains correctly.
- setIfDisabled(CONTINUE) on a DISABLED index: builds and marks readable (PASS).
- setIfWriteOnly(ERROR) on WRITE_ONLY_WITH_TRACKING: rejected, state kept (PASS).
- Tracking requested on an empty store: becomes READABLE, empty COUNT index (PASS).
- OnlineIndexerWriteOnlyWithTrackingTest already covers tracking semantics heavily (user io, races, restart, multi-target mixes).
## Rejected
- (none yet)
## Untried
- Multi-target: primary READABLE (ifReadable=CONTINUE) and a secondary DISABLED: handleState returns before checking secondaries; buildIndex
  "succeeds" while the secondary stays disabled. Need a spec reference first.
- Multi-target: primary DISABLED with ifDisabled=CONTINUE, secondary partly built WRITE_ONLY -> mismatch exception expected?
- markReadableIfBuilt on a WRITE_ONLY_WITH_TRACKING index: tracking table erased by markIndexReadable (fine?), check a later rebuild.
- setIfReadable(REBUILD) with setUseWriteOnlyWithTracking: clearAndMarkIndexWriteOnly -> WRITE_ONLY -> WRITE_ONLY_WITH_TRACKING path.
- Time limit (validateTimeLimit) with toWait included: off-by-wait when limit is exactly reached.
- maybePresetRecordsRangeAsync with typed records and a subset of record types: ranges outside marked built; progress counts.
