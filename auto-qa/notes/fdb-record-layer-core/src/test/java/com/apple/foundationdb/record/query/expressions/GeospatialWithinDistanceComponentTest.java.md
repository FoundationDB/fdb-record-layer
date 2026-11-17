# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/query/expressions/GeospatialWithinDistanceComponentTest.java
## Visits
- 2026-11-17 14daf66b7
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/expressions/GeospatialWithinDistanceComponent.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/expressions/DoubleValueOrParameter.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/GeospatialRTreeScanComparisons.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/RecordQueryPlanner.java (planGeospatial)
## Explored
- Nested use (Query.field("location").matches(geo over concat(latitude, longitude))): CANDIDATE BUG, evalMessage evaluates coordinatesExpression against rec, ignoring message, so returns null. GeoPointWithinDistanceComponent (spatial) uses message.
- Parameter-bound center/radius via planner index scan vs per-record eval: agree (non-boundary points).
- planHash: swapped center lat/lon, value vs parameter, different parameter names all give different hashes; equal param components equal; toString shows $param: PASS.
- Record without location: excluded by filter and by NOT(filter) (null semantics): PASS.
## Rejected
- Comparing index plan vs no-index plan by reopening the store with a different hook: stale meta-data exception (test issue); use filter.eval on loaded records instead.
## Untried
- Integer/Long-typed parameter binding: DoubleParameter.getValue casts (Double) -> ClassCastException; no spec says other Number types are accepted, so weak.
- Missing binding: Bindings.get throws MissingBindingException in evalMessage while javadoc of getValue says "a double value or null".
- OneOfThem over a repeated message field with geo child (same rec-vs-message root cause as the nested candidate).
- Plan serialization round trip of GeospatialRTreeScanComparisons with parameter values (toProto/fromProto) and planHash equality after round trip.
- Grouped index with grouping equality bound by a parameter (equalsParameter) via planGeospatial.
- coordinatesExpression with FanOut (repeated location): evaluateSingleton throws; check validate rejects it.
