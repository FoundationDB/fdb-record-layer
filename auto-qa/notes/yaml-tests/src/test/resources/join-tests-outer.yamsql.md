# Notes: yaml-tests/src/test/resources/join-tests-outer.yamsql
## Visits
- 2026-11-19 71430039d
## Production code
- fdb-relational-core/src/main/java/com/apple/foundationdb/relational/recordlayer/query/visitors/QueryVisitor.java (visitOuterJoin, wrapOperandsForOuterJoin)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/expressions/OuterJoinExpression.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/rules/EliminateNullOnEmptyRule.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/cascades/Traversal.java
- docs/sphinx/source/reference/sql_commands/DQL/JOIN.rst, docs/sphinx/source/reference/Joins.rst
## Explored
- NULL join keys on both sides of LEFT JOIN (NULL = NULL never matches), duplicate matches: correct
- ON-clause predicate on the preserved side only (l.v = 'a'): correct, preserved rows kept
- Null-accepting COALESCE WHERE on null-supplying side: correct
- NOT (col = const) over null-padded column (UNKNOWN rejects): correct
- WHERE IS NULL over derived table wrapping a LEFT JOIN: correct
- Chained LEFT JOIN correlated to the null-supplying side of the first: correct
- LEFT JOIN against a grouped derived table with a NULL group: correct
- maxRows=2 continuations over LEFT JOIN ORDER BY preserved PK: correct
- RIGHT JOIN with OR / IS NULL WHERE on null-supplying side: correct
- LEFT/RIGHT JOIN followed by any INNER/CROSS/comma join (also when the outer join is wrapped in a derived table):
  CANDIDATE BUG, planner throws IllegalArgumentException "Node refN[...] is not an element of this graph" from
  Traversal.pruneUnreferencedRefs (CascadesPlanner.ExploreGroup). Test block left-then-inner-join-auto-qa.
## Rejected
- (none so far; early failures were yamsql matcher mistakes: rows with several NULLs or duplicate !l values need !pos)
## Untried
- Outer join on the right of an INNER/CROSS join where the outer join is in a derived table (`l2 CROSS JOIN (… LEFT JOIN …)`)
- LEFT JOIN with ON referencing a scalar/EXISTS subquery
- DISTINCT over LEFT JOIN with several null-padded rows (dedup of NULLs)
- RIGHT JOIN followed by LEFT JOIN (mixed directions)
- LEFT JOIN inside a CTE / view, then filtered on the null-supplying side
- Continuations (maxRows=1) splitting the matches of a single preserved row
- Note: `./gradlew :yaml-tests:test` fails to start the external (mixed-mode) server here; use `:yaml-tests:quickTest`
