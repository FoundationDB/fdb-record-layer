# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerMutualTest.java
## Visits
- 2026-11-19 bf040b1e3
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
## Rejected
- Single-element boundary list ([null]): throws ValidationException "No ranges to build". A one-key list defines no fragment; no javadoc promises it works.
## Untried
- Mutual [A,B] -> mutual [B,A] (same set, different order): stamps differ, so it fails with PartlyBuiltException, while MULTI_TARGET -> MUTUAL accepts any order (containsAll). Only report if a doc says order is irrelevant.
- Mutual on an empty store with preset boundaries, and with boundaries for a different record type prefix.
- Boundaries containing Tuple.from() (empty tuple packs to empty bytes, below RangeSet first key {0x00}).
- anyJumper: deterministic conflict simulation (two indexers on the same fragment) to check "Mutual indexing failure - third iteration" is not hit when work remains.
- Split long records (splitLongRecords) with boundaries that fall between a record's split keys.
