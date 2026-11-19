# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerIndexFromIndexTest.java
## Visits
- 2026-11-19 bf040b1e3
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingByIndex.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingBase.java (iterateRangeOnly, handleCursorResult[Reverse], setIndexingTypeOrThrow)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java (indexingCatcher, fallback)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java (updateWriteOnlyByIndex, evaluateSingletonIndexKey)
## Explored
- SUM target by non-unique source with null source values, crash after 2 chunks, updates moving records across the built boundary (both ends), deletes, inserts, then resume in opposite direction: SUM exact (pass)
- Same with covering (keyWithValue) source, source key containing the primary key, and both combined, forward and reverse: exact (pass)
- Source disabled mid-build with forbidRecordScan: fails with UnusableSourceIndexException, target stays WRITE_ONLY; after rebuilding the source, continuing by it gives an exact result (pass)
- maxWriteLimitBytes(1) forcing a continuation after every record, online build and inline rebuildIndex: no double counting (pass)
- READABLE_UNIQUE_PENDING source (unique with duplicates) is accepted as a source and all records are indexed (pass)
## Rejected
- Filtered/sparse source index missing records: setSourceIndex javadoc says it is the caller's responsibility that the source covers all records.
- Partly built by index, source becomes unusable, fallback allowed: fails with PartlyBuiltException (cannot continue a BY_INDEX partial build by records unless ifMismatchPrevious=REBUILD). Appears intended, no spec says otherwise.
- TakeoverTypes.BY_INDEX_TO_BY_RECORDS: mentioned only in allowTakeoverContinue javadoc example; no such enum value (doc typo, not a behavior bug).
## Untried
- Forward/reverse scan where the cursor stops for a non-exhausted reason before returning any item (lastResult.get().get() on exhausted result in postIterateRangeOnly): find a way to trigger (time limit / scan limit).
- Source whose record type has a record-type-key prefix in its primary key, or a multi-type store where only one type is indexed.
- WRITE_ONLY_WITH_TRACKING target built by index with concurrent updates (updateWriteOnlyWithTracking path).
- Source index lastModifiedVersion bumped (metadata change) during a paused by-index build with forbidRecordScan.
- Index maintenance filter on the store excluding records from both source and target (the "both entry keys null" path).
