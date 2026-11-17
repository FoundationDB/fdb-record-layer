# Auto QA bugs

## AQA-1: checkAnyOngoingOnlineIndexBuildsAsync treats stale heartbeats as active sessions
- status: open
- found: 2026-11-16
- test: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerAutoQATest.java#checkAnyOngoingOnlineIndexBuildsStaleHeartbeatReturnsFalse
- spec: OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(FDBRecordStore, Index, long) javadoc: "leasingMilliseconds max heartbeat age to be considered an active session"
- details: OnlineIndexer.java ~554 computes activeTime = now + lease and checks heartbeatTime < activeTime, which is
  true for every past heartbeat, so the lease is ignored. IndexingHeartbeat.checkSingleHeartbeat uses the correct
  check (now - heartbeatTime < lease).

## AQA-2: IndexingPolicy.Builder.allowTakeoverContinue(empty collection) throws IllegalArgumentException
- status: open
- found: 2026-11-16
- test: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerAutoQATest.java#allowTakeoverContinueEmptyCollectionAllowsNoConversion
- spec: IndexingPolicy.Builder#allowTakeoverContinue(Collection) javadoc: "Null or empty set will allow no conversion."
- details: EnumSet.copyOf throws on an empty collection that is not an EnumSet (e.g. List.of()). Low impact:
  allowTakeoverContinue(false) is a workaround.

## AQA-3: deleteRecordsWhere leaves uniqueness violations of deleted records behind
- status: rejected (see auto-qa/rejected.md on CAUGHT)
- found: 2026-11-16
- test: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexerUniqueIndexAutoQATest.java#deleteRecordsWhereRemovesViolationsOfDeletedRecords
- spec: FDBRecordStoreBase.deleteRecordsWhere javadoc: "Delete records and associated index entries matching a query filter"; IndexStateCommitCheck javadoc (READABLE_UNIQUE_PENDING becomes READABLE once violations are resolved)
- details: StandardIndexMaintainer.deleteWhere (~876) clears only the index range, not indexUniquenessViolationsSubspace.
  The leftover violations point at deleted records, so no write can resolve them and a READABLE_UNIQUE_PENDING
  index can never become READABLE. All other delete paths remove violations.
