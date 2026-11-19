# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/metadata/RecordMetaDataBuilderTest.java
## Visits
- 2026-11-19 71430039d
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/RecordMetaDataBuilder.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/RecordMetaData.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/MetaDataValidator.java
## Explored
- toProto of a multi-type index over two synthetic (unnested) types: CANDIDATE BUG. toProto (~746) only walks
  syntheticRecordType.getIndexes(), not getMultiTypeIndexes(), so the index proto has no record_type and is
  reloaded as a universal index; setRecords(proto).getRecordMetaData() then fails validation.
- removeIndex on an index of a synthetic type: CANDIDATE BUG. removeIndex (~1213) only cleans recordTypes, so the
  synthetic type keeps the index (and FDBRecordStore ~947 keeps maintaining it) while a FormerIndex with the same
  subspace key is added; toProto would NPE at RecordMetaData.java:748.
- Round trip of stored-type indexes (options, added/last-modified versions, subspace keys, multi-type, universal
  COUNT), former indexes, sinceVersion, explicit record type key, split/storeRecordVersions, getIndexesSince: PASS.
## Rejected
- (none)
## Untried
- loadProtoExceptRecords: an index proto listing both stored and synthetic types silently drops the stored types
  (only synthetic builders get the index). Check if the builder API can create such an index first.
- loadProtoExceptRecords adds a multi-synthetic index to each type's getIndexes() (not getMultiTypeIndexes), so
  recordTypesForIndex returns only the first type; becomes relevant once the toProto bug is fixed.
- getNextRecordTypeKey ignores stored types with negative explicit keys (collision with synthetic keys?).
- MetaDataEvolutionValidator with synthetic-type index changes and multi-type index record-type changes.
- setRecords(proto) when subspaceKeyCounter in proto is lower than existing index subspace keys.
