# Notes: fdb-relational-core/src/test/java/com/apple/foundationdb/relational/recordlayer/query/AstNormalizerTests.java
## Visits
- 2026-11-16 65f9c37bc
## Production code
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/AstNormalizer.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/PreparedParams.java
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/MutablePlanGenerationContext.java (value-free constraints)
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/Literals.java
## Explored
- Value-free (declared, unbound) named param in an all-constant IN list: cache key equals the bound one (PASS).
- Value-free named param used as the whole IN list (`in ?p`, declared BIGINT ARRAY): cache key equals the bound array one (PASS).
- Value-free STRING param: plan constraint equals the bound one (PASS).
- Value-free named param referenced twice (`id >= ?p and id <= ?p`): warmed constraint lacks the `@c13 EQUALS @c8` literal-equality member that the bound plan has -> CANDIDATE_BUG (violates MutablePlanGenerationContext:407-409 "must be equal member for member").
- Interval literal canonicalization: direction sign is folded into the literal value by SemanticAnalyzer.parseIntervalLiteral; existing tests cover qualifiers. Not re-tested.
- Byte literals hash into parameterHash via Objects.hash(byte[]) (identity hash), so the same query gives a different parameter hash per parse. Parameter hash is only compared for continuations, and regular SELECTs never validate it against a fresh parse (EXECUTE CONTINUATION reuses the continuation's binding hash), so no visible effect found.
- OPTIONS(...) tokens are not in the canonical string; only PLAN RIGHT DEEP feeds the cache key via planner config. DRY RUN / SNAPSHOT / LOG are execution-time. No issue.
## Rejected
- Value-free parameter referenced twice lacks the equality constraint a bound value has: verifier REJECTED. The equality comes from value-based literal dedup; the warmed plan is more general and its weaker constraint is correct, and PhysicalPlanEquivalence still reuses it for later bindings. At most a missed optimization.
- Plan-constraint comparisons for queries with OR or IN (`id in (?p, 2)`, `id = 1 or id = 2`) through the offline PlanGenerator with a hand-built RecordLayerSchemaTemplate: even plain bound queries fail with VerifyException in Reference.getResultType (PredicateToLogicalUnionRule). Harness artifact of the offline template, not reachable as a value-free issue; would need a real store (EmbeddedRelationalExtension) to test.
## Untried
- Value-free param inside an all-constant IN list, planned against a real store: planning adds a value-free literal inside the array literal scope (Literals.addValueFreeLiteral while startArrayLiteral is open) - check array literal contents / constraint vs bound.
- Value-free param in `EXECUTE CONTINUATION ?p`: visitContinuationAtom asserts `instanceof byte[]` and gives an internal error instead of a clear message.
- Value-free param in a temporary function body, then a query referencing the function (normalizeAst re-normalizes the routine description with routine.getPreparedParams()).
- `IN (1, (NULL))`: rejectNullItems only descends single-child nodes; check whether SemanticAnalyzer.validateInListItems also misses the parenthesized NULL.
- Interval value with charset prefix (`INTERVAL _utf8'1' DAY`): AstNormalizer decodes it differently from ExpressionVisitor (which rejects charset) - error code/message difference.
