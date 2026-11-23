# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStoreTest.java
## Visits
- 2026-11-23 c5e862619
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStoreBase.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java (deleteWhere)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/SplitHelper.java
## Explored
- deleteRecordsWhere with STRING prefix "a" vs sibling "a\u0000b": CANDIDATE BUG. Records range uses
  subspace(prefix).range() (sibling record survives), but StandardIndexMaintainer.deleteWhere (~876) clears
  [pack(prefix), strinc(pack(prefix))), which also covers "a\u0000b" (02 61 00 FF 62 00). The sibling's VALUE index
  entries are deleted and its grouped COUNT becomes 0. Tests: deleteRecordsWhereStringPrefixKeepsIndexEntriesOfNulSuffixedPrefix,
  deleteRecordsWhereStringPrefixKeepsCountOfNulSuffixedPrefix. Same family as AQA-22 (KeySpacePath).
- Overwriting a split record with a small one: no stale split keys (PASS).
- Two concurrent saveRecordAsync(ERROR_IF_EXISTS) of the same pk in one transaction (preload cache + write lock): exactly one fails (PASS).
- ERROR_IF_RECORD_TYPE_CHANGED with union store (MySimpleRecord2 vs MySimpleRecord3, same string pk): throws, old record kept (PASS).
## Rejected
- getSnapshotRecordCount(PATH, scalar) on a COUNT index added with addIndex("MyRecord", ...): throws
  AggregateFunctionNotSupportedException (needs a universal/record-count index); used evaluateAggregateFunction instead.
## Untried
- deleteRecordsWhere with a BYTES prefix (e.g. [0x01] vs [0x01, 0x00, ...]) and other index types (RANK, SUM, MAX_EVER, TEXT, permuted min/max) for the same strinc issue.
- deleteRecordsWhere with a NUL-suffixed prefix while the index is WRITE_ONLY_WITH_QUEUE (queued DELETE_WHERE replay).
- Old version format (useOldVersionFormat) version range clear on deleteRecordsWhere.
- Record versions: versionstamp ordering of several saves in one transaction; saveRecord with explicit incomplete version and storeRecordVersions=false.
- deleteRecord return value for a record deleted earlier in the same transaction (preload cache negative entries).
- Split record exactly at SPLIT_RECORD_SIZE and SPLIT_RECORD_SIZE+1 with omitUnsplitRecordSuffix.
