# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/indexes/GeospatialRTreeIndexTest.java
## Visits
- 2026-11-17 14daf66b7
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/GeospatialRTreeIndexMaintainer.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/GeospatialRTreeIndexHelper.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/GeospatialRTreeIndexMaintainerFactory.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/RTreeIndexHelper.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/GeospatialRTreeScanBounds.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/GeospatialRTreeScanComparisons.java
- fdb-extensions/src/main/java/com/apple/foundationdb/async/rtree/RTree.java (Rectangle.unionWith keeps null lows)
## Explored
- Null coordinates (record without location) in a multi-level tree: CANDIDATE BUG, overlapsMbrApproximately NPEs on null MBR low (GeospatialRTreeScanBounds.java:154).
- Radius 0 with center having more digits than precision: CANDIDATE BUG, point stored at the exact center is not returned (containsPosition compares decoded cell with raw center).
- Update of a record's location in a multi-level tree: PASS.
- Circle reaching the north pole, points on far meridians: PASS.
- Grouped index, TupleRange.ALL, limit-1 continuation across groups: PASS.
## Rejected
- (none yet)
## Untried
- Stored longitudes outside [-180, 180] (e.g. 190) vs pruning rectangles (normalization is only applied to the center; no doc says stored values must be normalized).
- Boundary rounding: point exactly on the circle edge along a meridian vs Math.round in FixedRectangle.
- NaN / infinite radius or center longitude (NaN normalizes to NaN).
- GEOSPATIAL_RTREE_PRECISION_DIGITS edge values (0, 15, invalid strings -> NumberFormatException vs RecordCoreArgumentException).
- Grouped index with a null grouping value; prefixRange spanning a subset of groups.
- Reverse scan properties (likely ignored).
- Planner (GeospatialWithinDistanceComponent) with parameter radius bound to null / negative.
- deleteWhere on an ungrouped index (canDeleteWhere with empty prefix) clearing the node-slot subspace.
