# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpacePathTest.java
## Visits
- 2026-11-19 71430039d
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpacePath.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpacePathImpl.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpaceDirectory.java (areEqual, valueHashCode)
## Explored
- Recent 60-day changes to keyspace package: Javadoc/style only.
- equals/hashCode for Integer -1 vs Long -1L: equal but hashCodes differ (valueHashCode uses Objects.hashCode, Long.hashCode(-1)=0) -> CANDIDATE_BUG.
- BYTES path toTuple -> resolveFromKey -> toPath: equal and same hash code; passes.
- deleteAllDataAsync / hasDataAsync range [pack, strinc(pack)): includes sibling "a\u0000b" (and byte[] siblings with 0x00 suffix) -> CANDIDATE_BUG (both methods, one root cause).
## Rejected
- (none yet)
## Untried
- toString(Tuple) uses Objects.equals: byte[] values or Integer-vs-Long show a spurious "x->x" arrow (weak spec).
- exportAllData on a leaf path: KeyValueCursor withSubspace may exclude a key exactly equal to the path tuple (hasData includes it) - inconsistency.
- toResolvedPathAsync(context, key) where the key's next element matches no subdirectory of a non-leaf directory.
- Same NUL-suffix issue for BYTES values ([0x01] vs [0x01,0x00]) and for listSubdirectoryAsync of a parent.
- equals/hashCode with DirectoryLayerDirectory wrapped paths and KeySpacePathWrapper subclasses.
