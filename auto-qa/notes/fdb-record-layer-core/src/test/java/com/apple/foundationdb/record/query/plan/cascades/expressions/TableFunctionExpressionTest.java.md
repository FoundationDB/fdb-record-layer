# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/expressions/TableFunctionExpressionTest.java
## Visits
- 2026-11-18 56547041b
- 2026-11-27 915aa0e3f
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/expressions/TableFunctionExpression.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/ExplodeValue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/StreamingValue.java (impliesCandidateValue)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/MatchInfo.java (requiresExistential, tryMerge)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/expressions/SelectExpression.java (existential absorbs the flag)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/translation/MaxMatchMap.java (identityOf)
## Explored
- subsumedBy, equal order flags, collections compare equal but element types differ (LONG vs INT): reported, REJECTED
  (type-blind semantic equality is by design). Since #4881 the relaxed path has no element-type check either.
- semanticEquals/equalsWithoutChildren between preserving and non-preserving explodes: false both ways (PASS).
- semantic hash codes of explodes equal under the binding alias map: equal (aliases not in hash) (PASS).
- translateCorrelations keeps preserveOrder flag and yields an explode equal to the candidate (PASS).
- reverse-sorted query vs order-preserving / ascending sorted candidate: not subsumed; same sorted explode is (PASS).
- (#4881) translateCorrelations of a distinct explode, with and without simplification, keeps it distinct, and a
  query explode matched against the translated one still requires an existential (PASS).
- (#4881) distinct vs non-distinct TableFunctionExpression: not equal by equals/semanticEquals/equalsWithoutChildren
  either way; distinct ones equal under the alias map with equal hash codes (PASS). hashCodeWithoutChildren and
  planHash ignore `distinct` on purpose (commit message), which keeps equals/hashCode consistent.
- (#4881) requiresExistential of the leaf match: true only for a distinct candidate, false for equal and
  order-preserving candidates (PASS). ExplodeValueTest already covers preserving/sorted query vs distinct candidate,
  other collection, and a distinct query explode (VerifyException).
- Read only: toProto/evalAsStream of a distinct explode silently drop / ignore `distinct` (javadoc says it "cannot be
  evaluated or serialized"); the commit message says it is planner-only by design, so not worth reporting.
## Rejected
- subsumedBy matches explodes with equal order flags but different element types: verifier REJECTED. Semantic equality is type-blind throughout the planner (FieldValue by ordinal, QuantifiedObjectValue by alias), the equal-flags path behaved the same before #4619, and the type check was deliberately added only to the relaxed path. A hardening idea, not a documented contract.
## Untried
- End-to-end (needs an FDB-backed test class, e.g. in provider/foundationdb/query): Cascades oneOfThem on a value
  index over `repeater` with a record holding [5, 5, 7]; the index is used and the record is returned once. And a
  nested fan-out index (parent FanOut -> child FanOut) where two parents hold the same child value.
- WindowedIndexExpansionVisitor / VectorIndexExpansionVisitor keep isFanOutDistinct() = false; check whether a rank
  index on a fan-out field also collapses equal elements into one entry (then its candidate explode should be distinct).
- SelectExpression.subsumedBy: a select over the TFE with a for-each quantifier keeps requiresExistential; with an
  existential quantifier it is cleared (needs a PartialMatch in the partialMatchMap).
- TableFunctionExpression.equalsWithoutChildren with different result types (same root cause as the rejected idea).
- translateCorrelations where the collection simplifies to something else (e.g. an array constructor).
- End-to-end: SQL UNNEST matched against an UnnestedRecordType index; ordering properties do not claim ordinal order
  for the non-preserving query explode.
