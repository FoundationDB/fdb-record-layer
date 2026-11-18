# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/expressions/TableFunctionExpressionTest.java
## Visits
- 2026-11-18 56547041b
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/expressions/TableFunctionExpression.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/ExplodeValue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/translation/MaxMatchMap.java (identityOf)
## Explored
- subsumedBy, equal order flags, collections compare equal but element types differ (LONG vs INT): CANDIDATE_BUG. The
  semanticEquals fast path in isSubsumedBy has no element-type check (only the preserve/non-preserve path does), and
  ExplodeValue/FieldValue/QuantifiedObjectValue equality ignores types, so it matches. Pre-existing on the equal path
  (exactlySubsumedBy had the same behavior); inconsistent with the type check added in 4bdf2632e.
- semanticEquals/equalsWithoutChildren between preserving and non-preserving explodes: false both ways (PASS).
- semantic hash codes of explodes equal under the binding alias map: equal (aliases not in hash) (PASS).
- translateCorrelations keeps preserveOrder flag and yields an explode equal to the candidate (PASS).
- reverse-sorted query vs order-preserving / ascending sorted candidate: not subsumed; same sorted explode is (PASS).
## Rejected
- subsumedBy matches explodes with equal order flags but different element types: verifier REJECTED. Semantic equality is type-blind throughout the planner (FieldValue by ordinal, QuantifiedObjectValue by alias), the equal-flags path behaved the same before #4619, and the type check was deliberately added only to the relaxed path. A hardening idea, not a documented contract.
- (none)
## Untried
- TableFunctionExpression.equalsWithoutChildren returns true for explodes with different result types (same root
  cause as the candidate); check whether memoization can merge them in a real plan.
- translateCorrelations with shouldSimplifyValues=true where the collection simplifies to something else (e.g. an
  array constructor): ensure the cast to StreamingValue holds.
- End-to-end: SQL UNNEST query matched against an UnnestedRecordType index; check the plan's ordering properties do not
  claim ordinal order for the non-preserving query explode.
- subsumedBy with a non-TableFunctionExpression candidate and with a candidate StreamingValue that is not ExplodeValue
  (e.g. RangeValue).
