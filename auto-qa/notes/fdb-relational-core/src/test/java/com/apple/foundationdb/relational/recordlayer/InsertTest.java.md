# Notes: fdb-relational-core/src/test/java/com/apple/foundationdb/relational/recordlayer/InsertTest.java
## Visits
- 2026-11-19 71430039d
## Production code
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/EmbeddedRelationalStatement.java (executeInsert, ensureTransaction)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/AbstractEmbeddedStatement.java (executeInternal error path)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/EmbeddedRelationalConnection.java (canCommit, rollbackInternal)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/RecordTypeTable.java (toDynamicMessage)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/storage/BackingRecordStore.java (insert)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/plans/RecordQueryInsertPlan.java
## Explored
- INSERT ... SELECT from the same table (rest_no + 100, WHERE rest_no < 1000): PASS, no Halloween problem, count 3.
- SQL multi-row VALUES with duplicate PK in auto-commit: PASS, 23505 and nothing inserted.
- Direct-access executeInsert(list) with duplicate PK in auto-commit: PASS, 23505 and nothing inserted.
- SQL insert of STRUCT, ARRAY of STRUCT, STRING ARRAY (incl. empty arrays), update count 2: PASS.
- SQL INSERT of integer literal into STRING column: throws (PASS); test later replaced.
- SQL multi-row VALUES with duplicate PK with autoCommit=false, then commit: CANDIDATE BUG, the first row of the
  failed statement is committed. AbstractEmbeddedStatement.executeInternal only rolls back when conn.canCommit()
  (auto-commit), so partial writes of the failed statement stay in the user transaction.
## Rejected
- INSERT INTO t(col list) SELECT ...: "setting column ordering for insert with select is not supported"
  (QueryVisitor.visitInsertStatement); deliberate limitation, use full-column SELECT instead.
## Untried
- Direct-access executeInsert(list) with a duplicate in an explicit transaction (autoCommit=false), then commit:
  ensureTransaction does not roll back, so earlier structs likely get committed (same root cause as the candidate).
- Direct access: explicit null / unset for NOT NULL array column silently becomes [] (toDynamicMessage).
- Direct access: addInt for a BIGINT column (no coercion, CANNOT_CONVERT_TYPE expected per TODO).
- SQL numeric overflow: INSERT 9223372036854775808 into BIGINT, INTEGER column with a BIGINT value out of range.
- INSERT ... SELECT that produces duplicate PKs among its own output rows; INSERT ... SELECT with a unique index.
- REPLACE_ON_DUPLICATE_PK with a list containing the same key twice: returned count.
