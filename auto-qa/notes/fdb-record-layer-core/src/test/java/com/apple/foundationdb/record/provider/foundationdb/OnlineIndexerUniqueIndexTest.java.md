# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerUniqueIndexTest.java
## Visits
- 2026-11-16 65f9c37bc
- 2026-11-23 c5e862619
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexStateCommitCheck.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.java (updateIndexStateToReadable, checkAndUpdateBuiltIndexState, resolveUniquenessViolation, RecordsWhereDeleter)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java (removeUniquenessViolationsAsync, checkUniqueness, deleteWhere)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java (allowUniquePendingState)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/IndexEntry.java (keyContainsNonUniqueNull)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/Key.java (NullStandin semantics)
## Explored
- resolveUniquenessViolation(index, key, null) with 2/3/5 duplicates (pipelined deletes) in READABLE_UNIQUE_PENDING: index auto-marked READABLE at commit. PASS
- resolveUniquenessViolation keeping one of 3/5 duplicates: auto READABLE, scrub clean. PASS
- Two concurrent transactions resolving the last two distinct duplications: one conflicts (or index becomes READABLE); never stuck RUP with 0 violations. PASS
- Partial resolution keeps RUP; allowUniquePendingState rebuild and markIndexReadable don't make it READABLE; resolving the rest auto-marks READABLE. PASS
- deleteRecordsWhere on a RUP unique index prefixed by the delete key: violations of deleted records are NOT removed (StandardIndexMaintainer.deleteWhere clears only the index range). CANDIDATE_BUG (test deleteRecordsWhereRemovesViolationsOfDeletedRecords). Pre-existing, not from #4828, but leaves the RUP index stuck.
- Already covered upstream by FDBRecordStoreUniqueIndexTest (commit 65f9c37bc): resolve by delete/update one per txn, two indexes together, new violation in same txn, write-only states stay write-only, replaced-index interplay.
- Diff 65f9c37bc..c5e862619 of OnlineIndexer.java: only checkAnyOngoingOnlineIndexBuildsAsync (AQA-1 fix); no effect on unique builds.
- NullStandin.NULL: several records missing the unique field build straight to READABLE, no violations. PASS
- NullStandin.NULL_UNIQUE: missing fields are duplicates -> RUP with violations under Tuple(null); resolving keeps one, auto READABLE. PASS
- Crash part way (config loader throws), writes in between (new dup beyond built range, update removing a dup), continue with allowUniquePendingState: violation set matches final records. PASS
- Multi-target crash + continuation with allowUniquePendingState: dup unique index RUP with 10 violations, non-unique and dup-free unique targets READABLE. PASS
## Rejected
- deleteRecordsWhere does not remove the uniqueness violations of deleted records (AQA-3): rejected by the team, see AQA-3 in auto-qa/rejected.md on the caught branch
- (none)
## Untried
- deleteAllRecords on a RUP index: violations cleared but state stays RUP (javadoc says index states are kept; probably intended, check rebuildAllIndexes interplay).
- Two FDBRecordStore instances on the same subspace in one context: IndexStateCommitCheck is bound to the first instance's cached state.
- Resolve last violation, then markIndexDisabled / markIndexWriteOnly in the same txn: commit check must skip it.
- Index with replacedBy where the unique-pending replacement is resolved via resolveUniquenessViolation(null) (pipelined path).
- OnlineIndexer multi-target build where one unique target ends RUP and the duplication is resolved while another target is still building.
- Multi-target build where two unique targets both have duplicates, one resolved before the build ends.
- Compound unique key with one NULL and one non-null component (keyContainsNonUniqueNull on partial nulls).
- Unique index on a repeated field (FanOut) where one record repeats the same value twice (self-duplicate must not be a violation).
