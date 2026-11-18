# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/plan/cascades/values/ValueTranslationTest.java
## Visits
- 2026-11-18 56547041b
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/translation/MaxMatchMap.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/translation/TranslationMap.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/Value.java (pullUp, semanticEquals)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/QuantifiedValue.java (pullUp)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/RecordConstructorValue.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/values/FieldValue.java
## Explored
- translateQueryValueMaybe with a candidate that exposes the matched value twice (rcv(t'.a, t'.a)): CANDIDATE BUG,
  Iterables.getOnlyElement in MaxMatchMap.translateQueryValueMaybe (~263) throws IllegalArgumentException
  "expected one element but was: <C._0, C._1>" instead of returning a value or Optional.empty().
  Same getOnlyElement-on-pullUp pattern exists in PullUp.java (~185); not tested there.
- identityOf + translateQueryValueMaybe: yields qov(C) as expected.
- identityOf vs compute with a single ranged-over alias and an alias-map equivalence: same map and translation.
- nested rcv candidate (rcv(rcv(a.q, a.r) as x, b as y)) with query rcv(b.t, a.q): translates to rcv(C.y.t, C.x.q).
- translateCorrelations twice with the same rebase map: idempotent.
## Rejected
- Type-boundary matching via QOV semanticEquals ignoring type: documented/accepted in commit 4bdf2632e message
  ("a quantified value disregards its type"); not a spec violation for MaxMatchMap.
## Untried
- PullUp.pullUp (PullUp.java ~175) through a pull-through value with duplicate columns: same getOnlyElement crash?
- MatchInfo aggregate pull-up (~535) with duplicated query result columns.
- adjustMaybe chained over two levels with reordered fields.
- compute with ValueEquivalence carrying value-level (isDefinedEqual) equivalences and constraints in the result.
- translateQueryValueMaybe where a query leaf is correlated to a ranged-over alias but unmatched (expect empty).
