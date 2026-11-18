# Notes: fdb-relational-core/src/test/java/com/apple/foundationdb/relational/recordlayer/query/cache/RelationalPlanCacheTests.java
## Visits
- 2026-11-18 56547041b
## Production code
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/cache/RelationalPlanCache.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/cache/MultiStageCache.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/cache/QueryCacheKey.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/cache/Reducer.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/cache/PhysicalPlanEquivalence.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/PlanGenerator.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/PlannerConfiguration.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/AstNormalizer.java (normalizeAst: key construction)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/visitors/MetadataPlanVisitor.java
## Explored
- PLAN_CACHE_* metrics end-to-end (primary/secondary/tertiary miss, tertiary hit) and getStats numHits/numMisses over a lookup sequence: correct.
- OPTIONS (PLAN CACHE WRITE ONLY): no lookup, WRITE_ONLY_STORE counted, equal-constraint entry replaced, plain query later served from it: correct.
- DISABLED_PLANNER_RULES and INDEX_FETCH_METHOD connection options give separate secondary keys: correct.
- Query-level OPTIONS (DRY RUN, LOG QUERY, SNAPSHOT) are excluded from the key but are applied per execution from PlanGenerator options, so not a key problem.
- CURRENT_PLAN_HASH_MODE: PhysicalQueryPlan.withExecutionContext takes the mode from the new execution context: fine.
- Metadata plans (DESCRIBE SCHEMA, SHOW DATABASES) are cached (only NOCACHE/DDL/INSERT bypass) with a noConstraint tertiary key and withExecutionContext returning this. DESCRIBE SCHEMA <unqualified> captures the connection's dbUri at plan time -> CANDIDATE BUG: a second database with the same template gets the first database's DATABASE_PATH (test describeSchemaInSecondDatabaseReportsThatDatabase).
## Rejected
- MultiStageCache class javadoc says secondary cache is read-TTL but code uses expireAfterWrite: Options.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS javadoc says write-TTL, docs conflict, so not reportable.
- Recreating a dropped schema template with same name gets version 1 again and could reuse stale plans: DROP SCHEMA TEMPLATE doc says behavior with existing schemas is undefined/experimental; no spec.
- getStats().numSecondaryEntries(key) etc. call mainCache.getIfPresent, which bumps primary hit stats and refreshes the primary access-TTL: no contract says stat reads are side-effect free (CacheStatistics only says "mostly a delegation to CacheStats").
## Untried
- SHOW DATABASES (no prefix) also captures getDbUri() at plan time; check whether a cached plan from DB1 lists the wrong databases for DB2.
- Universal indexes are dropped from the readable-index set in PlanContext.getReadableIndexes; two stores differing only in which universal index is unreadable share a key.
- Tertiary/secondary removal listeners: secondary eviction invalidates the primary entry by key even if a new secondary cache was installed under that key meanwhile (needs concurrency).
- PLAN_CACHE_WRITE_ONLY as a connection option combined with NOCACHE.
