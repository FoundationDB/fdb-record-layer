# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/GeospatialRTreeScanBoundsTest.java
## Visits
- 2026-11-17 14daf66b7
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/GeospatialRTreeScanBounds.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/GeospatialRTreeIndexMaintainer.java (encodePoint, no range validation of stored coords)
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/GeospatialRTreeIndexHelper.java (getScale, max 15 digits)
## Explored
- Randomized pruning/containment consistency (scales 1, 10, 1e3, 1e7; radius 10 m..30,000 km; points near/wrapping around the center): no case where containsPosition accepts a point that overlapsMbrApproximately prunes. Rounding of rectangle bounds is safe because stored coords are integers.
- Radius greater than half the circumference: full band, antipode and pole/antimeridian corner contained. PASS.
- Radius 0 with center lon +180 (normalized to -180): point stored at (lat, +180) is contained (distance 0) but pruned, since the single rectangle is [-180,-180]. CANDIDATE_BUG.
- NaN center latitude: passes the [-90, 90] range check (comparisons with NaN are false); bounds silently match nothing and scan only the (0,0) cell. CANDIDATE_BUG (weaker; javadoc states range, no explicit throws).
## Rejected
- withinDistance accepts a NaN center latitude: verifier REJECTED. The javadoc states a caller precondition ("in [-90, 90]") but no @throws; NaN is caller misuse with unspecified result, and matching nothing is reasonable.
- (none yet)
## Untried
- Positive radius whose east/west edge lands exactly on +/-180 (eastLongitude == 180 not split, so points stored at -180 on the boundary are pruned); needs a radius that yields exact equality.
- Infinite / NaN center longitude: normalizeLongitude returns NaN; rectangles encode NaN as 0.
- NaN / infinite radius: Math.max(0, NaN) = NaN; rectangles collapse to (0,0); containsPosition false.
- Stored longitudes outside [-180, 180] (e.g. 190) are not validated by the maintainer; containment (haversine) accepts them but rectangles never cover them.
- End-to-end store test via GeospatialRTreeIndexMaintainer scan with antimeridian/pole data and continuations.
- Precision 15 digits: floating error at rectangle boundaries.
