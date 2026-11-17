# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/MetaDataPlanContextSyntheticIndexTest.java
## Visits
- 2026-11-17 14daf66b7
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/MetaDataPlanContext.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/RecordMetaData.java (getSyntheticRecordTypesFromRecordTypes)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/JoinedRecordType.java, UnnestedRecordType.java, SyntheticRecordType.java (getStoredRecordTypes)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/RecordQueryPlannerConfiguration.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/PlannerConfiguration.java
## Explored
- Joined type index offered only when both constituents are queried (forRecordQuery single/multi/ALL types, forRootReference): PASS
- Disabled synthetic index not offered with flag on: PASS
- IndexQueryabilityFilter (DEFAULT/TRUE/FALSE) and allowedIndexes applied to synthetic indexes: PASS
- Multi-type index on two unnested types of the same parent: NOT offered with flag on (syntheticRecordTypeIndexes uses getIndexes(), not getMultiTypeIndexes()) -> candidate bug
- Relational PlannerConfiguration equals/hashCode/withPlanRightDeep/buildRecordQueryPlannerConfiguration carry the new flag (code review only): looks correct
## Rejected
- Multi-type indexes on synthetic record types are not offered with considerSyntheticRecordTypeIndexes: verifier REJECTED. The flag documents "the readable indexes of every synthetic record type" (getIndexes), multi-type indexes are a separate list, and the feature is experimental groundwork (#4918). A possible improvement, not a documented contract.
- (none)
## Untried
- Multi-type index spanning a covered and an uncovered synthetic type (e.g. UnnestedMap + joined with OtherRecord) must not be offered for a query on OuterRecord only (relevant once multi-type synthetic indexes are added).
- Self-joined type (both constituents OuterRecord) offered for OuterRecord query via MetaDataPlanContext (RecordMetaDataBuilderTest covers only getSyntheticRecordTypesFromRecordTypes).
- Outer-joined constituent: javadoc says still required; check behavior once that changes.
- RecordQueryPlannerConfiguration fromProto/toProto round trip with the new flag combined with other flags.
- End-to-end CascadesPlanner planning with flag on over a store with unnested indexes: query must not fail (ListKeyExpression primary key skip).
