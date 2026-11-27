# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/rules/PartitionBinarySelectRuleTest.java
## Visits
- 2026-11-27 915aa0e3f
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/rules/PartitionBinarySelectRule.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/expressions/Comparisons.java (swapComparisonType, evalComparison)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/predicates/RangeConstraints.java (ofComparisonMaybe)
## Explored
- Semantic equivalence of reoriented ValuePredicate join criteria (EQUALS, NOT_EQUALS, <, <=, >, >=, NOT_DISTINCT_FROM, IS_DISTINCT_FROM, both orientations) under 3VL with NULL/1/2 operands, by evaluating the pushed predicates of both yields: PASS (reorientedValuePredicateIsEquivalent).
- Same for sargable PredicateWithValueAndRanges with a single comparison (evaluated through toResidualPredicate): PASS.
- Range with two comparisons against the other side is pushed unchanged to both sides: PASS.
- Inequality join criterion with an explode (correlated) right side is reoriented with the flipped type (t.b > g.x -> g.x < t.b): PASS.
- Code review: swap() only handles plain ValuePredicate + ValueComparison; DistanceRankValueComparison has non-swappable types; ExistentialValuePredicate excluded; AND/OR/NOT untouched; PWVAR with >1 comparisons untouched. Unconstrained RangeConstraints (null evaluable, no deferred) is not reachable via public API, so the "comparisons.size()==1 but 2 ranges" case only arises with emptyRange (2 comparisons) -> not an issue.
## Rejected
- NULL join keys turned into index scan ranges matching NULL (e.g. reoriented `q.y > NULL` scan): known upstream issue #4170 (ScanComparisons.toTupleRange), documented in cross-join-tests.yamsql; not a rule bug.
- ValueComparison.eval returns TRUE when the comparand is COMPARISON_SKIPPED_BINDING (asymmetric after a swap): only for parameter bindings of IN-joins, comparand of a swapped predicate is a quantifier value, not reachable.
## Untried
- End-to-end (FDB-backed) join query with reversed inequality join predicate against an index on the inner side, with mixed types requiring promotion (INT vs BIGINT) to check the promote stays on the right operand after the swap.
- Predicate whose value is constant and comparand correlated to both quantifiers (e.g. 5 = t.a + tau.alpha), check pushed side and swap.
- Interaction with PredicatePushDownRule / SelectMergeRule after reorientation (predicate pushed into a nested select where the alias is translated).
