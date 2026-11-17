# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpaceDirectoryTest.java
## Visits
- 2026-11-16 65f9c37bc
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpaceDirectory.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/keyspace/KeySpace.java
## Explored
- Recent 30-day changes to keyspace package: Javadoc/style only (no behavior change).
- addSubdirectory with equal-content BYTES constants (different arrays): accepted (uses Objects.equals, reference equality) -> CANDIDATE_BUG.
- addSubdirectory with LONG constants 5L and Integer 5: accepted (Objects.equals(Long, Integer) false) -> CANDIDATE_BUG.
- isValueValid/validateValue with Integer 5 on LONG constant 5L: returns false although path add accepts it (areEqual) -> CANDIDATE_BUG.
- Reverse listSubdirectoryAsync with returned-row limit resumed via continuation across transactions: correct.
## Rejected
- (none yet)
## Untried
- isValueValid with constant Integer and value Long (reverse direction of the same bug).
- KeyType range bounds for listing: BOOLEAN (0x26..0x28), UUID, FLOAT/DOUBLE NaN / -0.0 values with valueRange endpoints.
- listSubdirectoryAsync with valueRange plus reverse plus continuation.
- pathFromKey with keySize < key.size() (remainder computed from key.size() not keySize).
- toTree / toString output for BYTES constants (prints array identity, not content).
- KeySpaceDirectory with NULL type: isConstant() is always true since NULL anyValue is null.
