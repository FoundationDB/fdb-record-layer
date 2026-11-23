# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerTest.java
## Visits
- 2026-11-16 65f9c37bc
- 2026-11-23 c5e862619
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
- AQA-1 fix (#4929, heartbeatTime > now - lease): stale-only heartbeats (incl. age == lease+1 and time 0) -> false; mixed stale+fresh -> true; 2-arg overload uses DEFAULT_LEASE_LENGTH_MILLIS; unparseable heartbeat -> false; lease 0 -> false for past heartbeats; heartbeat 5 min in the future -> true; instance method uses configured setLeaseLengthMillis. All PASS. AQA-1 regression test passes now.
- Note: IndexingHeartbeat.checkHeartbeats treats heartbeats >1 day in the future as bad data (inactive), while checkAnyOngoingOnlineIndexBuildsAsync treats any future heartbeat as active. Inconsistent, but no javadoc on checkAnyOngoing covers future heartbeats, so not reported.
- clearIndexingHeartbeats maxIteration counting: already covered in OnlineIndexingHeartbeatTest / IndexingHeartbeatLowLevelTest, skipped.
## Rejected
- setPendingWriteQueueIndexesMaxDrainAttempts "Must be positive" not validated: 0/negative just means one attempt, no exception is promised - not a clear spec violation.
- OnlineIndexOperationConfig.Builder.setInitialLimit javadoc says default initial limit is DEFAULT_LIMIT, but effective default is maxLimit; base builder javadoc says non-positive -> max limit. Docs conflict with each other, not a solid reference.
## Untried
- Duplicate target indexes [A, A] with a source index: rejected as multi-target before deduplication (validateIndexSetting).
- ifMismatchPrevious=REBUILD after a partial by-records build, then by-index request: verify stamp becomes BY_INDEX and index readable.
- blockIndexBuilds with ttlSeconds expiry, then buildIndex without allowUnblock.
- OnlineIndexer.getLimit() with setInitialLimit > setLimit should equal max limit.
- checkAnyOngoingOnlineIndexBuildsAsync with negative / Long.MIN_VALUE lease (now - lease overflows -> everything active); no spec, low value.
