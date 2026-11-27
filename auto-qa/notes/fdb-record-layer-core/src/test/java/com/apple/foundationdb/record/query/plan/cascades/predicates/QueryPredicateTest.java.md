# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/predicates/QueryPredicateTest.java
## Visits
- 2026-11-19 71430039d
- 2026-11-27 915aa0e3f
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
- RangeConstraints diff 71430039d..915aa0e3f: fromProto refactor (no behavior change) + new ofComparisonMaybe; Placeholder.translateLeafPredicate now passes shouldSimplifyValues. No effect on AQA-23/AQA-24 (both still fail).
- RangeConstraints proto round trip / ofComparisonMaybe (EQUALS, IS_NULL, deferred STARTS_WITH; NOT_EQUALS -> empty): PASS.
- Compiled range merging (IS NULL & NOT NULL, IS NULL & =5, NOT NULL & =5, adjacent <5/>=5, <=5/>5, <=5/>=5 single point, disjoint) and encloses with null ranges: PASS.
- RangeConstraints.equals/hashCode/semanticHashCode: CompilableRange has no equals/hashCode, so they are identity-based; semanticEquals true but semanticHashCode differs -> CANDIDATE BUG (rangeConstraintsSemanticHashCodeConsistentWithSemanticEquals).
- PredicateWithValueAndRanges.eval of 5 with range (> NULL-literal ValueComparison): null comparand becomes Tuple(null), i.e. lowest value, so eval says TRUE while residual says NULL -> CANDIDATE BUG (predicateWithValueAndRangesGreaterThanNullComparandIsNotTrue). compileTimeEval also builds SimpleComparison with a null comparand (@Nonnull).
## Rejected
- PredicateWithValueAndRanges.eval returns TRUE for value 5 against a `> NULL` range while its residual predicate gives NULL: verifier REJECTED. RangeConstraints is defined by tuple ordering (null sorts lowest, NOT_NULL maps to the same greaterThan range), the class javadoc says it is for index matching and not evaluable at runtime, and nothing documents that eval() must follow 3VL for a null comparand. The @Nonnull comparand passed null in compileTimeEval is an annotation nit.
- PredicateWithValueAndRanges.eval skips non-compile-time (deferred) ranges and returns FALSE ('abc' STARTS_WITH 'a'): explicit `continue` by design; only used for plan-constraint checks where FALSE is conservative (cache miss); no spec to cite.
- OrPredicate.toValueWithRangesMaybe with a PredicateWithValueAndRanges child always returns empty (builder has no comparisons): missed optimization only, not a correctness bug.
- foldPredicateAtNull(NOT(q IS NULL)) returns UNKNOWN: conservative, documented as a possible outcome.
## Untried
- PredicateWithValueAndRanges.eval with NOT_NULL range and NULL value (returns null instead of FALSE; filter-equivalent, weaker).
- PredicateWithValueAndRanges.hashCodeWithoutChildren ignores ranges ("TODO why not the ranges"): only a hash-quality issue.
- OrPredicate implication (impliesCandidatePredicateMaybe) with overlapping / IS NULL ranges.
- ConstantFoldingMultiConstraintPredicateRule with contradictory ranges involving NULL.
- translateCorrelations idempotence for AND/OR with duplicate children after translation (and() may collapse them).
- yamsql: `WHERE x > ?` with ? bound to NULL (and filtered index `WHERE x IS NOT NULL`): null comparand maps to Tuple(null), so OrPredicate/PWVR implication may consider `x > NULL` equal to `x IS NOT NULL` without compensation; check for non-empty results.
- RangeConstraints duplicates in ImmutableSet of PWVR ranges (identity equality): OR of two identical ranges not deduplicated.
- RangeConstraints.compileTimeEval returns `this` when any comparand is a ConstantObjectValue (looks inverted vs. the name); check a mixed COV + LiteralValue range.
