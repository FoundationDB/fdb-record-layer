# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerSimpleTest.java
## Visits
- 2026-11-19 bf040b1e3
- 2026-11-23 c5e862619 (OnlineIndexer only changed by the AQA-1 fix; building code unchanged)
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
- setIfReadable(REBUILD) + setUseWriteOnlyWithTracking on a READABLE COUNT index: rebuilt READABLE, count exact (PASS).
- setIfWriteOnly(REBUILD) + tracking on a WRITE_ONLY_WITH_TRACKING index with tracked user writes: clear + re-mark tracking works,
  count exact, maintained afterwards (PASS).
- markReadableIfBuilt on a fully built WRITE_ONLY_WITH_TRACKING index with later user writes: true, READABLE, count exact (PASS).
- markReadable with allowUniquePendingState on a built unique index with duplicates -> READABLE_UNIQUE_PENDING; after resolving the
  duplicate, markReadableIfBuilt (default policy) -> READABLE, entries correct (PASS).
- Code read: handleStateAndDoBuildIndexAsync returns before checking secondaries when the primary is READABLE and ifReadable=CONTINUE;
  ifReadable javadoc says "CONTINUE: (default) do nothing", so consistent.
## Rejected
- markReadableIfBuilt on a built unique WRITE_ONLY index with duplicates should return false: it throws RecordIndexUniquenessViolation.
  The javadoc ("true if all ... readable or marked readable and false otherwise") only describes the return value, and
  store.markIndexReadable documents the throw; not strong enough. Test kept, accepting either false or the violation.
  Note: markReadableIfBuilt also ignores policy.allowUniquePendingState (always store.markIndexReadable) - allowUniquePendingState
  javadoc only talks about buildIndex, so not reported.
## Untried
- Multi-target: primary READABLE (ifReadable=CONTINUE) and a secondary DISABLED: handleState returns before checking secondaries; buildIndex
  "succeeds" while the secondary stays disabled. Need a spec reference first.
- Multi-target: primary DISABLED with ifDisabled=CONTINUE, secondary partly built WRITE_ONLY -> mismatch exception expected?
- Multi-target markReadableIfBuilt with one unique target that has duplicates: single transaction, so the other built target is not
  marked readable either (compare buildIndex, where allowUniquePendingState javadoc says others are marked readable).
- Multi-target buildIndex with allowUniquePendingState=false and one target with duplicates: others READABLE (javadoc claim), check it.
- Continued build (WRITE_ONLY) with tracking requested: states left as is (logged); verify COUNT stays exact when user writes
  happen during the plain write-only continuation.
- Time limit (validateTimeLimit) with toWait included: off-by-wait when limit is exactly reached.
- maybePresetRecordsRangeAsync with typed records and a subset of record types: ranges outside marked built; progress counts.
