# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/predicates/QueryPredicateTest.java
## Visits
- 2026-11-19 71430039d
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/AndPredicate.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/OrPredicate.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/AndOrPredicate.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/NotPredicate.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/ConstantPredicate.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/ValuePredicate.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/PredicateWithValueAndRanges.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/RangeConstraints.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/simplification/*.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/planning/BooleanPredicateNormalizer.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/ValueEquivalence.java (set semanticEquals)
## Explored
- PredicateWithValueAndRanges.eval with NULL value and IS NULL range: returns null (early return), residual predicate says TRUE -> CANDIDATE BUG (predicateWithValueAndRangesIsNullRangeOnNullValueIsTrue).
- AndOrPredicate.equalsForChildren under alias map: ValueEquivalence.semanticEquals(Set, Set) checks sizes and left->right only (non-injective); AND(q1.a=x, q2.a=x) equals AND(q2.a=x, q2.b=y) under q1->q2, but not in reverse; hash codes also differ -> CANDIDATE BUG.
- Constant folding of p OR NOT p with p = (NULL = 'x'): stays NULL. PASS.
- Default rule set (De Morgan, NotOverComparison) over NULL operands preserves eval. PASS.
- ConstantPredicate NULL/TRUE/FALSE proto round trip. PASS.
- Rule review (no test): Absorption, Annulment, Identity, De Morgan, distributive normal forms are valid in Kleene logic; NotOverComparisonRule only inverts =,<,<=,>,>= (safe under NULL); no rule for NOT(constant) (conservative, not a bug).
## Rejected
- OrPredicate.toValueWithRangesMaybe with a PredicateWithValueAndRanges child always returns empty (builder has no comparisons): missed optimization only, not a correctness bug.
- foldPredicateAtNull(NOT(q IS NULL)) returns UNKNOWN: conservative, documented as a possible outcome.
## Untried
- PredicateWithValueAndRanges.eval with NOT_NULL range and NULL value (returns null instead of FALSE; filter-equivalent, weaker).
- PredicateWithValueAndRanges.eval silently skips non-compile-time ranges and returns FALSE instead of NULL/unknown.
- PredicateWithValueAndRanges.hashCodeWithoutChildren ignores ranges ("TODO why not the ranges"): only a hash-quality issue.
- OrPredicate implication (impliesCandidatePredicateMaybe) with overlapping / IS NULL ranges.
- ConstantFoldingMultiConstraintPredicateRule with contradictory ranges involving NULL.
- translateCorrelations idempotence for AND/OR with duplicate children after translation (and() may collapse them).
