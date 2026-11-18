# Notes: fdb-relational-core/src/test/java/com/apple/foundationdb/relational/recordlayer/query/AstNormalizerTests.java
## Visits
- 2026-11-16 65f9c37bc
- 2026-11-18 56547041b (revisit after 1ee946b42 "Remove parameter names from the canonical query string")
## Production code
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/AstNormalizer.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/PreparedParams.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/MutablePlanGenerationContext.java (value-free constraints, literal dedup)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/Literals.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/metadata/RecordLayerSchemaTemplate.java (temp routines now sorted by normalized description)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/PlanValidator.java (binding hash check for continuations)
## Explored
- Value-free (declared, unbound) named param in an all-constant IN list: cache key equals the bound one (PASS).
- Value-free named param used as the whole IN list (`in ?p`, declared BIGINT ARRAY): cache key equals the bound array one (PASS).
- Value-free STRING param: plan constraint equals the bound one (PASS).
- Value-free named param referenced twice (`id >= ?p and id <= ?p`): warmed constraint lacks the literal-equality member a bound plan has (rejected earlier, see Rejected).
- 2026-11-18, after names were dropped from the canonical string:
  - Stored query `id >= p and id <= p` (PREPARE FOR p IS NOT NULL), client sends `?lo`/`?hi` with different values: hits the warmed plan (tertiary hit) and returns the right rows (PASS). Value-free literals get one COV per token, no name-based dedup, so the old "missing equality" concern is harmless here.
  - Same stored query, client sends two plain `?`: hit and right rows (PASS).
  - `id > ? and id < ?hi` then `id > ?lo and id < ?` on one connection: second run is a cache hit and binds by position (PASS).
  - Temp function body `id > ?a and id < ?b` then `id > ?b and id < ?a` (swapped names, new values) in a later transaction: right rows (PASS).
  - Parameter hash keeps the names (`Objects.hash("?name", value)`), canonical string does not. Only used by PlanValidator/EmbeddedRelationalStatement continuation checks; a continuation from `?a` replayed with `?b` is rejected on purpose (comment in visitPreparedStatementParameter). Not tested as a bug.
  - Dedup paths (getFirstValueDuplicateMaybe / getFirstDuplicateOfConstantIdMaybe) skip value-free literals and are value-based, not name-based.
- Interval literal canonicalization: direction sign folded by SemanticAnalyzer.parseIntervalLiteral; covered by existing tests.
- Byte literals hash via Objects.hash(byte[]) (identity hash); no visible effect found (see earlier visit).
- OPTIONS(...) tokens are not in the canonical string; only PLAN RIGHT DEEP feeds the key. No issue.
## Rejected
- Value-free parameter referenced twice lacks the equality constraint a bound value has: verifier REJECTED. The warmed plan is more general and its weaker constraint is correct. Re-checked after 1ee946b42: a client with two distinct names now reaches that plan and gets correct rows, so still not a bug.
- Plan-constraint comparisons for OR / IN through the offline PlanGenerator with a hand-built RecordLayerSchemaTemplate: VerifyException in Reference.getResultType even for bound queries. Harness artifact; use EmbeddedRelationalExtension instead.
## Untried
- Struct parameters of different struct types at the same place share the canonical `?` (type not in the string): check the OfType constraint keeps them apart (applies to unnamed `?` too).
- Docs example with one name `?flag` in two temp functions is covered by StoredQueriesTest; untried: same name used in a temp function and in the outer query with different values in the same transaction.
- Value-free param in `EXECUTE CONTINUATION ?p`: visitContinuationAtom asserts `instanceof byte[]` and gives an internal error instead of a clear message.
- `IN (1, (NULL))`: rejectNullItems only descends single-child nodes; check SemanticAnalyzer.validateInListItems for the parenthesized NULL.
- Interval value with charset prefix (`INTERVAL _utf8'1' DAY`): AstNormalizer decodes it differently from ExpressionVisitor.
- Two temp functions whose normalized descriptions sort differently from their raw descriptions (RecordLayerSchemaTemplate now sorts by normalized description): check literal import order / scope is still stable.
