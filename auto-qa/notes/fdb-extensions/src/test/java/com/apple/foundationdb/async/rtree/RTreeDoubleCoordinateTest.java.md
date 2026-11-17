# Notes: fdb-extensions/src/test/java/com/apple/foundationdb/async/rtree/RTreeDoubleCoordinateTest.java
## Visits
- 2026-11-17 14daf66b7
## Production code
- fdb-extensions/src/main/java/com/apple/foundationdb/async/rtree/RTree.java (Point, Rectangle, scan, insertOrUpdate, delete)
- fdb-extensions/src/main/java/com/apple/foundationdb/async/rtree/RTreeHilbertCurveHelpers.java
## Explored
- Insert/validate/full scan/delete of 2D points from special doubles (+-0.0, +-inf, NaN, +-MAX, +-MIN_VALUE, 1e+-300) across BY_SLOT/BY_NODE, node slot index on/off, storeHilbertValues false: PASS (scan in (hilbert, key) order, storage empty after delete).
- Box scan pruned with Rectangle.isOverlapping vs brute-force Rectangle.contains, incl. -0.0/0.0 boundaries: PASS.
- Mixed Long/Double/null coordinates in one dimension: tree valid, Hilbert-ordered scan, continuation resume, delete: PASS. (Tuple order puts all longs before all doubles; Hilbert uses numeric sortable longs; inconsistent but harmless since MBR pruning uses Tuple order everywhere.)
- 1D Hilbert order -inf < -MAX < MAX < +inf < NaN: PASS.
- toSortableLong: bijective on doubles (NaN canonicalized via doubleToLongBits); null and Long.MIN_VALUE collide on Hilbert value, as do long 0 and double 0.0 (harmless, key disambiguates).
## Rejected
- Resumed scan(lastHilbertValue, lastKey) returning only items after the watermark: scan resumes at leaf granularity and returns earlier items of the same leaf; javadoc says items in leaves are not filtered and RTreeIndexHelper filters with compareHilbertValueAndKey > 0. Test filters now.
## Untried
- Rectangle.area() with infinite or null coordinates throws (NumberFormatException/NPE); area() unused in production and no javadoc, so not reportable unless a caller appears.
- Float coordinates (Tuple.from(1.5f)) truncate to long in Hilbert value; javadoc only promises Long/Double.
- Concurrent inserts of double points in separate transactions (conflict/retry behavior).
- Large random double datasets (thousands of points, splits/fuses across many levels) with random delete subsets.
