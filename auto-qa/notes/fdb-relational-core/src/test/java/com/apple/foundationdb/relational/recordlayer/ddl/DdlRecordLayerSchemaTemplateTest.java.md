# Notes: fdb-relational-core/src/test/java/com/apple/foundationdb/relational/recordlayer/ddl/DdlRecordLayerSchemaTemplateTest.java
## Visits
- 2026-11-18 56547041b
## Production code
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/metadata/RecordLayerSchemaTemplate.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/visitors/DdlVisitor.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/ddl/CreateTemporaryFunctionConstantAction.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/ddl/DropTemporaryFunctionConstantAction.java
## Explored
- RecordLayerSchemaTemplate.toBuilder(): does not copy storeRowVersions (copies name, version, enableLongRows, intermingleTables). CANDIDATE BUG (toBuilderPreservesStoreRowVersions).
- CREATE TEMPORARY FUNCTION on a store_row_versions=true template: bound template loses the option, "__ROW_VERSION" becomes an unknown column for the rest of the transaction. CANDIDATE BUG (createTemporaryFunctionKeepsRowVersionColumn). Same root cause; DROP TEMPORARY FUNCTION and OfflineStoredQueriesProcessor (replaceInvokedRoutine) also go through toBuilder.
- Duplicate index name across two tables: rejected (MetaDataException mapped to SYNTAX_OR_ACCESS_VIOLATION, not INDEX_ALREADY_EXISTS); template not created. PASS.
- Quoted table names differing only in case ("t", "T"): both created. PASS.
- Recent change 1ee946b42: temp routines sorted by normalized description instead of description; no issue found yet.
## Rejected
- Builder.removeInvokedRoutine(missing name) throws NPE instead of UNDEFINED_FUNCTION (inverted condition at RecordLayerSchemaTemplate.java ~618): real code smell, but unreachable through SQL (DropTemporaryFunctionConstantAction checks existence first) and the method has no javadoc, so no spec to cite.
- Expecting INDEX_ALREADY_EXISTS for duplicate index names across tables: no doc says which code; only RecordLayerTable uses it for same-table duplicates.
## Untried
- Two temporary functions whose normalized descriptions are equal (sort ties) -> getTransactionBoundMetadataAsString stability / plan cache key collisions.
- toBuilder() also drops cachedMetadata: check that metadata built after a temp function keeps index versions / last-modified versions (index state, record type keys).
- Insert inside a transaction after CREATE TEMPORARY FUNCTION on a store_row_versions template: are versions stored (metadata rebuilt with storeRecordVersions=false)?
- Enum with duplicate values ('A','A') and enum with zero values: error code.
- WITH OPTIONS repeated (store_row_versions=true, store_row_versions=false): last wins silently?
- Index on a nested struct field and on a missing column: error codes.
