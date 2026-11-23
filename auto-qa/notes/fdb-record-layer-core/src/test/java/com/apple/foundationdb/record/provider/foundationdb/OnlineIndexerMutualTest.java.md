# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerMutualTest.java
## Visits
- 2026-11-19 bf040b1e3
- 2026-11-23 c5e862619
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingMutuallyByRecords.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingBase.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java (IndexingPolicy.setMutualIndexingBoundaries, shouldAllowTypeConversionContinue)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexing/IndexingHeartbeat.java
## Explored
- Mutual crash, then records deleted/added while WRITE_ONLY, then a peer with different, unsorted boundaries finishes: COUNT index value matches the record count (exactly once). PASS.
- Two crashed mutual peers, then a finishing peer: no heartbeats left after READABLE (crashed peers also clear their own heartbeat in buildIndexAsync's whenComplete; markIndexReadable clears all heartbeats via clearReadableIndexBuildData). PASS.
- Boundaries without null endpoints ([30, 60]): index is not marked READABLE while partly built (fails with "third iteration"); a full-range peer then completes. PASS.
- Mutual multi-target crash, then each target continued by single BY_RECORDS with MUTUAL_TO_SINGLE (plus new records): COUNT correct. PASS.
- Code read: unsorted boundaries still cover the full space (fragments are [b_i, b_i+1) from -inf to +inf; reversed fragments are empty and skipped).
- Code read: mutual heartbeats (allowMutual) never check other heartbeats, so a mutual session may start while an exclusive one is active; documented as "a takeover may cause it to fail".
- Revisit c5e862619: only change since bf040b1e3 is the AQA-1 fix in OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync. Mutual indexing never calls it (mutual heartbeats skip peer checks; IndexingHeartbeat.checkSingleHeartbeat already used the correct age check).
- Crashed mutual build + hand-written peer heartbeat: fresh -> checkAnyOngoing true and single BY_RECORDS (MUTUAL_TO_SINGLE) fails with SynchronizedSessionLockedException; stale (3x lease) -> checkAnyOngoing false and the takeover completes; no heartbeats remain. PASS.
- Fresh and stale foreign mutual heartbeats do not block a mutual indexer, which finishes the peer's fragments. PASS.
- Boundaries with Tuple.from() (empty tuple, packs to empty bytes below RangeSet's first key): build completes, COUNT correct. PASS.
- Empty store with preset boundaries, including a string-typed boundary: READABLE, count 0, later saves counted. PASS.
- Split long records (250KB, splitLongRecords) with primary-key boundaries, crash after 4 iterations at limit 1, then finish: COUNT and VALUE entries correct. PASS. (A primary-key boundary always packs below all split suffixes of that key, so a valid boundary cannot fall between split keys.)
## Rejected
- Mutual [A,B] -> mutual [B,A]: not tried as a bug; IndexingMutuallyByRecords javadoc says "each indexer must be called with the exact same arguments" and docs/sphinx/source/IndexBuilding.md says "Every participant must use the same policy", so order mismatch failing is allowed.
- Boundaries that fall between a record's split keys (e.g. Tuple.from(5L, 1L) with a one-field primary key): not a primary key, so outside the documented "set of primary keys" contract.
- Single-element boundary list ([null]): throws ValidationException "No ranges to build". A one-key list defines no fragment; no javadoc promises it works.
## Untried
- anyJumper: deterministic conflict simulation (two indexers on the same fragment) to check "Mutual indexing failure - third iteration" is not hit when work remains.
- Stand-alone mergeIndexes (checkNoActiveIndexingSession) with a fresh vs stale mutual peer heartbeat.
- Multi-target mutual where the primary index heartbeat is stale but a secondary target's heartbeat is fresh: checkAnyOngoingOnlineIndexBuilds only looks at the primary index.
- Mutual with typed-record primary keys (recordType prefix) plus preset boundaries that lie outside the record type's range (maybePresetRecordsRangeAsync).
