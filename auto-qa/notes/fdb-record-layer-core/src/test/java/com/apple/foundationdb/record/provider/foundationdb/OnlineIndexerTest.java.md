# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerTest.java
## Visits
- 2026-11-16 65f9c37bc
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexOperationConfig.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexOperationBaseBuilder.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexing/IndexingHeartbeat.java
## Explored
- OnlineIndexerTest is an abstract helper base with no @Test methods; companion is standalone (copies setup helpers), does not extend it.
- OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(store, index, lease): CANDIDATE BUG - compares heartbeatTime < now + lease, so any heartbeat (even stale) counts as active. IndexingHeartbeat.checkHeartbeats uses age < lease correctly.
- IndexingPolicy.Builder.allowTakeoverContinue(Collection): CANDIDATE BUG - EnumSet.copyOf(List.of()) throws IllegalArgumentException, javadoc says empty allows no conversion.
- IndexingPolicy.toBuilder round trip: all fields preserved (PASS).
- indexingCatcher CONTINUE/REBUILD paths (commit 87fea2a43): read, each adjustment attempted once; no obvious loop.
## Rejected
- setPendingWriteQueueIndexesMaxDrainAttempts "Must be positive" not validated: 0/negative just means one attempt, no exception is promised - not a clear spec violation.
- OnlineIndexOperationConfig.Builder.setInitialLimit javadoc says default initial limit is DEFAULT_LIMIT, but effective default is maxLimit; base builder javadoc says non-positive -> max limit. Docs conflict with each other, not a solid reference.
## Untried
- Duplicate target indexes [A, A] with a source index: rejected as multi-target before deduplication (validateIndexSetting).
- ifMismatchPrevious=REBUILD after a partial by-records build, then by-index request: verify stamp becomes BY_INDEX and index readable.
- blockIndexBuilds with ttlSeconds expiry, then buildIndex without allowUnblock.
- OnlineIndexer.getLimit() with setInitialLimit > setLimit should equal max limit.
- clearIndexingHeartbeats(minAge, maxIteration) boundary: maxIteration counts.
