# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexScrubberTest.java
## Visits
- 2026-11-19 bf040b1e3
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexScrubber.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexScrubbing.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexScrubbingTools.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/ValueIndexScrubbingToolsMissing.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/ValueIndexScrubbingToolsDangling.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingBase.java (iterateRangeOnly, handleCursorResult, iterateAllRanges)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingThrottle.java (buildCommitRetryAsync)
## Explored
- OnlineIndexerTest is an abstract base without @Test methods; the companion extends it.
- Commit-conflict retry of a scrubbing transaction: CANDIDATE BUG. IndexScrubbing.reportIssues runs in whenComplete of indexScrubRangeOnly, i.e. before the runner commits, so a transaction that fails at commit and is retried counts its issues twice (scrubMissingIndexEntries returned 2 for 1 missing entry). Comment in reportIssues says "report these issues only after their transaction was completed successfully". Conflict injected by an IndexMaintenanceFilter hook that rewrites (same value) an index key already read non-snapshot by the missing scrubber.
- Several limited runs (setLimit 4, setEntriesScanLimit 4, allowRepair=false) on missing scrubbing: every record scanned exactly once per cycle, every issue reported once, range set cleared at end, no index writes. PASS.
- Fan-out (repeated) value index: one missing entry per record counted once per record, repair restores all; deleted records yield one dangling issue per entry, all removed. PASS.
- Index with null values (concat num_value_2, str_value_indexed), dangling with limit 2 and scan-limit runs, continuation keys containing nulls: PASS.
## Rejected
- (none yet)
## Untried
- Same retry double count for scanCounter in checkScanLimit: recordsScanned in buildCommitRetryAsync is shared across retries, so entriesScanLimit may stop early after a retry (same root cause as the candidate).
- Dangling scrubber only checks that the record exists, not that the record still produces that index key (stale entry pointing to an existing record with a different value is not detected). Check docs before reporting.
- NPE risk: if the cursor stops for a non-row-limit reason (time/byte scan limit) before returning any item, getKeyFromCursorResult(null) in IndexScrubbing.indexScrubRangeOnly.
- Scrubbing a READABLE_UNIQUE_PENDING unique index with duplicates (isScannable accepts it): missing repair path for unique index.
- Scrubbing with setRecordTypes restricted to a subset of a multi-type index.
- Filtered index (IndexMaintenanceFilter SOME / index predicate): missing scrubber must not report filtered-out entries; dangling scrubber behavior for entries the filter would drop.
