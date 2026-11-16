# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerUniqueIndexTest.java
## Visits
- 2026-11-16 65f9c37bc
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexStateCommitCheck.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.java (updateIndexStateToReadable, checkAndUpdateBuiltIndexState, resolveUniquenessViolation, RecordsWhereDeleter)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java (removeUniquenessViolationsAsync, checkUniqueness, deleteWhere)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java (allowUniquePendingState)
## Explored
- resolveUniquenessViolation(index, key, null) with 2/3/5 duplicates (pipelined deletes) in READABLE_UNIQUE_PENDING: index auto-marked READABLE at commit. PASS
- resolveUniquenessViolation keeping one of 3/5 duplicates: auto READABLE, scrub clean. PASS
- Two concurrent transactions resolving the last two distinct duplications: one conflicts (or index becomes READABLE); never stuck RUP with 0 violations. PASS
- Partial resolution keeps RUP; allowUniquePendingState rebuild and markIndexReadable don't make it READABLE; resolving the rest auto-marks READABLE. PASS
- deleteRecordsWhere on a RUP unique index prefixed by the delete key: violations of deleted records are NOT removed (StandardIndexMaintainer.deleteWhere clears only the index range). CANDIDATE_BUG (test deleteRecordsWhereRemovesViolationsOfDeletedRecords). Pre-existing, not from #4828, but leaves the RUP index stuck.
- Already covered upstream by FDBRecordStoreUniqueIndexTest (commit 65f9c37bc): resolve by delete/update one per txn, two indexes together, new violation in same txn, write-only states stay write-only, replaced-index interplay.
## Rejected
- (none)
## Untried
- deleteAllRecords on a RUP index: violations cleared but state stays RUP (javadoc says index states are kept; probably intended, check rebuildAllIndexes interplay).
- Two FDBRecordStore instances on the same subspace in one context: IndexStateCommitCheck is bound to the first instance's cached state.
- Resolve last violation, then markIndexDisabled / markIndexWriteOnly in the same txn: commit check must skip it.
- Index with replacedBy where the unique-pending replacement is resolved via resolveUniquenessViolation(null) (pipelined path).
- OnlineIndexer multi-target build where one unique target ends RUP and the duplication is resolved while another target is still building.
