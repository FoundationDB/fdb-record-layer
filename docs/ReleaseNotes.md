
# Release Notes

This document contains a log of changes to the FoundationDB Record Layer. It aims to include mostly user-visible changes or improvements. Within each minor release, larger or more involved changes are highlighted first before detailing the changes that were included in each build or patch version. Users should especially take note of any breaking changes or special upgrade instructions which should always be included as a preface to the minor version as a whole before looking at changes at a version-by-version level.

As the [versioning guide](Versioning.md) details, it cannot always be determined solely by looking at the version numbers whether one Record Layer version contains all changes included in another. In particular, bug fixes and backwards-compatible changes might be back-ported to or introduced as patches against older versions. To track when a patch version has been included in the main release train, some releases will say as a note that they contain all changes from a specific patch.

## 3.4

### Breaking Changes

Support for the Protobuf 2 runtime has been removed as of this version. All artifacts now use Protobuf version 3. Note that the choice of Protobuf runtime version is distinct from the choice of Protobuf message syntax, and that users wishing to retain Protobuf 2 behavior can still achieve the same semantics (including [optional field behavior]()) as long as they specify the syntax on their Protobuf file as `proto2`. Note that the Maven artifacts using Protobuf version 3 used to be suffixed with `-pb3`. Existing Protobuf 3 users must remove that suffix from their dependency declarations (e.g., `fdb-record-layer-core-pb3` should now be `fdb-record-layer-core`).

Starting with version [3.4.455.0](#344550), the semantics of `UnnestedRecordType` were changed in response to [Issue #2512](https://github.com/FoundationDB/fdb-record-layer/issues/2512). It was identified that extraneous synthetic records were being produced when one of the children was empty. This did not match the semantics of `FanOut` expressions, and so the unnesting calculation was changed. This means that any index on an existing `UnnestedRecordType` requires rebuilding to clear out any such entries from older indexes.

<!--
// begin next release
### NEXT_RELEASE

* **Bug fix** Fix 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Infinite recursion in `TreeLike#replaceLeavesMaybe` [(Issue #2884)](https://github.com/FoundationDB/fdb-record-layer/issues/2884)
* **Performance** Improvement 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)

// end next release
-->

### 3.4.540.0

* **Bug fix** TransformedRecordSerializer fails to compress when serialized record is very very small. [(Issue #2896)](https://github.com/FoundationDB/fdb-record-layer/issues/2896)

### 3.4.539.0

* **Bug fix** DimensionsKeyExpression not handled enough by QueryToKeyMatcher [(Issue #2846)](https://github.com/FoundationDB/fdb-record-layer/issues/2846)

### 3.4.537.0

* **Feature** MultidimensionalIndex update operation is not thread safe [(Issue #2849)](https://github.com/FoundationDB/fdb-record-layer/issues/2849)

### 3.4.536.0

* **Performance** Add timing breakdown to Repartitioned records log [(Issue #2886)](https://github.com/FoundationDB/fdb-record-layer/issues/2886)

### 3.4.533.0

* **Bug fix** Remove uncommitted version mutations during `deleteRecordsWhere` to avoid corrupting record stores if there are outstanding record saves when `deleteRecordsWhere` is called  [(Issue #2275)](https://github.com/FoundationDB/fdb-record-layer/issues/2275)
* **Bug fix** ensure that all compensations get planned with their necessary quantifiers [(Issue #2881)](https://github.com/FoundationDB/fdb-record-layer/issues/2881)
* **Performance** Lucene partition balancing: reduce the number of retries [(Issue #2878)](https://github.com/FoundationDB/fdb-record-layer/issues/2878)

### 3.4.532.0

* **Bug fix** Expose `Tuple`-based `MIN_EVER` and `MAX_EVER` indexes to Cascades [(Issue #2874)](https://github.com/FoundationDB/fdb-record-layer/issues/2874)
* **Bug fix** Log Repartitioned records after writing them [(Issue #2867)](https://github.com/FoundationDB/fdb-record-layer/issues/2867)
* **Bug fix** Lucene merges: false no merges found [(Issue #2864)](https://github.com/FoundationDB/fdb-record-layer/issues/2864)
* **Feature** Deprecate special IndexingByRecords functions [(Issue #2259)](https://github.com/FoundationDB/fdb-record-layer/issues/2259) 

### 3.4.531.0

* **Bug fix** LazyOpener throws more precise exceptions [(Issue #2852)](https://github.com/FoundationDB/fdb-record-layer/issues/2852)
* **Bug fix** Fix flaky test - testMutualIndexingWeirdBoundaries [(Issue #2854)](https://github.com/FoundationDB/fdb-record-layer/issues/2854)

### 3.4.530.0

* **Bug fix** Unordered union implementation pre condition is too strict [(Issue #2856)](https://github.com/FoundationDB/fdb-record-layer/issues/2856)

### 3.4.529.0

* **Bug fix** Lucene exception management [(Issue #2850)](https://github.com/FoundationDB/fdb-record-layer/issues/2850)

### 3.4.528.0

* **Bug fix** Fix sporadic timeout in test [(Issue #2839)](https://github.com/FoundationDB/fdb-record-layer/issues/2839)
* **Feature** IndexOperation for getting Lucene index metadata [(Issue #2826)](https://github.com/FoundationDB/fdb-record-layer/issues/2826)

### 3.4.527.0

* **Bug fix** `Value` subsumption should default to semantic equality [(Issue #2825)](https://github.com/FoundationDB/fdb-record-layer/issues/2825)
* **Bug fix** `Value` simplification is not constructing the top-level simplified expression correctly [(Issue #2837)](https://github.com/FoundationDB/fdb-record-layer/issues/2837)
* **Bug fix** Memoization is not behaving correctly in some edge cases [(Issue #2836)](https://github.com/FoundationDB/fdb-record-layer/issues/2836)

### 3.4.526.0

* **Bug fix** Fail fast when Batch GRV rate limit exceeded [(Issue #2813)](https://github.com/FoundationDB/fdb-record-layer/issues/2813)

### 3.4.524.0

* **Bug fix** `ArithmeticValue::semanticEquals` now includes the operator in its calculation [(Issue #2189)](https://github.com/FoundationDB/fdb-record-layer/issues/2189)
* **Bug fix** subsumedBy is only looking at value type in most cases [(Issue #2818)](https://github.com/FoundationDB/fdb-record-layer/issues/2818)

### 3.4.523.0

* **Feature** Support for basic arithmetic functions in added as default `FunctionKeyExpression`s with query support in both the old and Cascades planners [(Issue #2663)](https://github.com/FoundationDB/fdb-record-layer/issues/2663)

### 3.4.522.0

* **Feature** add Support for nested DynamicMessage in ConstantObjectValue [(Issue #2806)](https://github.com/FoundationDB/fdb-record-layer/issues/2806)

### 3.4.521.0

* **Performance** improve performance for index anding in AbstractDataAccessRule [(Issue #2804)](https://github.com/FoundationDB/fdb-record-layer/issues/2804)

### 3.4.520.0


### 3.4.519.0

* **Breaking change** New the representation of aggregate plans [(Issue #2763)](https://github.com/FoundationDB/fdb-record-layer/issues/2763)

### 3.4.518.0

* **Bug fix** `TextCollatorRegistryICU.getTextCollator` should freeze the `Collator` [(Issue #2798)](https://github.com/FoundationDB/fdb-record-layer/issues/2798)
* **Performance** Covering optimization with invertible index fields [(Issue #2801)](https://github.com/FoundationDB/fdb-record-layer/issues/2801)
* **Feature** Add more complete direction control to `LogicalSortExpression` [(Issue #2796)](https://github.com/FoundationDB/fdb-record-layer/issues/2796)

### 3.4.517.0

* **Feature** Key expression support for reverse ordering and nulls last [(Issue #2722)](https://github.com/FoundationDB/fdb-record-layer/issues/2722)

### 3.4.516.0

* **Feature** OnlineIndexer / OnlineIndexScrubber: move the sync session lease length to Config [(Issue #2794)](https://github.com/FoundationDB/fdb-record-layer/issues/2794)

### 3.4.515.0

* **Performance** The index scrubber now attempts to take a transaction time limit into account when deciding whether to commit early [(Issue #2787)](https://github.com/FoundationDB/fdb-record-layer/issues/2787)
* **Breaking change** Online indexer and index scrubber configuration has been extracted into its own top-level class [(PR #2788)](https://github.com/FoundationDB/fdb-record-layer/pull/2788)

### 3.4.514.0

* **Feature** Improve modeling of Ordering, OrderingPart [(Issue #2127)](https://github.com/FoundationDB/fdb-record-layer/issues/2127)

### 3.4.513.0

* **Bug fix** Fix out of bound exception with repartitioning [(Issue #2784)](https://github.com/FoundationDB/fdb-record-layer/issues/2784)
* **Bug fix** Only log repartitioning records when there's repartitioning to do [(Issue #2782)](https://github.com/FoundationDB/fdb-record-layer/issues/2782)

### 3.4.512.0

* **Bug fix** Make exceptions from LuceneIndexMaintainer more verbose [(Issue #2768)](https://github.com/FoundationDB/fdb-record-layer/issues/2768)
* **Feature** Merge partitions when they become too small [(Issue #2739)](https://github.com/FoundationDB/fdb-record-layer/issues/2739)
* **Feature** Include cause with AgilityContext already closed exception [(Issue #2774)](https://github.com/FoundationDB/fdb-record-layer/issues/2774)

### 3.4.511.0

* **Bug fix** Fix flaky test - break loop into smaller commits [(Issue #2756)](https://github.com/FoundationDB/fdb-record-layer/issues/2756)
* **Bug fix** FDBDirectoryLock won't fail if AgilityContext is flushed while closing [(Issue #2754)](https://github.com/FoundationDB/fdb-record-layer/issues/2754)
* **Feature** Reproduce abandoned FileLock due to conflict issue [(Issue #2731)](https://github.com/FoundationDB/fdb-record-layer/issues/2731)

### 3.4.510.0

* **Bug fix** The `ArithmeticValue` and `NumericAggregationValue` classes now uses a fixed `Locale` with `toUpperCase` when encapsulating a specified function by name to avoid mismatches when running in the Turkish locale [(Issue #2750)](https://github.com/FoundationDB/fdb-record-layer/issues/2750)
* **Bug fix** Inconsistent nullability status for Array's element type [(Issue #2732)](https://github.com/FoundationDB/fdb-record-layer/issues/2737)
* **Feature** Make Lucene block cache size configurable [(Issue #2749)](https://github.com/FoundationDB/fdb-record-layer/issues/2749)
* **Feature** Validate filelock on every transaction to reduce ttl [(Issue #2747)](https://github.com/FoundationDB/fdb-record-layer/issues/2747)

### 3.4.504.0


### 3.4.502.0

* **Feature** In-join and in-union plans now support using `ConstantObjectValue`s as the basis of their `InSource` [(Issue #2717)](https://github.com/FoundationDB/fdb-record-layer/issues/2717)

### 3.4.501.0



### 3.4.500.0

* **Bug fix** Error messages reported when a store is missing a header now indicate whether the store has record or index data correctly [(Issue #2715)](https://github.com/FoundationDB/fdb-record-layer/issues/2715)
* **Bug fix** Missing Lucene error wrapper when index is partitioned [(Issue #2725)](https://github.com/FoundationDB/fdb-record-layer/issues/2725)

### 3.4.498.0

* **Bug fix** Fix issue with LuceneLock exception [(Issue #2721)](https://github.com/FoundationDB/fdb-record-layer/issues/2721)
* **Feature** Add isScannable(), isReadableUniquePending() to RecordStoreState [(Issue #2718)](https://github.com/FoundationDB/fdb-record-layer/issues/2718)

### 3.4.497.0


### 3.4.496.0

* **Bug fix** Make Lucene lock error retryable[(Issue #2700)](https://github.com/FoundationDB/fdb-record-layer/issues/2700)
* **Feature** Test FDBDirectory consistency while cross-transaction merges are ongoing [(Issue #2701)](https://github.com/FoundationDB/fdb-record-layer/issues/2701)
* **Feature** Online Indexing: improve allowTakeoverContine to distinguish type conversion cases [(Issue #2703)](https://github.com/FoundationDB/fdb-record-layer/issues/2703)

### 3.4.495.0

* **Bug fix** Corrected field to index calculation resolving records from index entries with permuted min/max indexes [(Issue #2705)](https://github.com/FoundationDB/fdb-record-layer/issues/2705)
* **Bug fix** Lucene directory: treat close as a potential recovery path [(Issue #2707)](https://github.com/FoundationDB/fdb-record-layer/issues/2707)
* **Bug fix** Lucene: FileLock should handle close as a possible recovery path [(Issue #2692)](https://github.com/FoundationDB/fdb-record-layer/issues/2692)
* **Performance** Lucene: merges should run in default transaction priority [(Issue #2696)](https://github.com/FoundationDB/fdb-record-layer/issues/2696)
* **Breaking change** The default collation order is now based on the root locale instead of the system default. Users relying on a specific locale should update any indexes using ta collation function to include the locale in the function arguments before updating [(Issue #2678)](https://github.com/FoundationDB/fdb-record-layer/issues/2678)

### 3.4.494.0

* **Bug fix** LuceneIndexTest.findStartingPartitionTest(false) false to decode tuple [(Issue #2684)](https://github.com/FoundationDB/fdb-record-layer/issues/2684)
* **Bug fix** Graceful Degradation breaks when index is being built [(Issue #2668)](https://github.com/FoundationDB/fdb-record-layer/issues/2668)
* **Feature** Add logging information to the runner used when repartitioning [(Issue #2685)](https://github.com/FoundationDB/fdb-record-layer/issues/2685)
* **Feature** The `TransformedRecordSerializer` now validates that compressed data decompresses to the expected size [(Issue #2689)](https://github.com/FoundationDB/fdb-record-layer/issues/2689)
* **Feature** The `TransformedRecordSerializer` now includes checksums in compressed data that is validated during decompression [(Issue #2689)](https://github.com/FoundationDB/fdb-record-layer/issues/2691)
* **Feature** Updated the set of known protocol versions for client log event parsing to include FDB 7.3 [(Issue #2682)](https://github.com/FoundationDB/fdb-record-layer/issues/2682)

### 3.4.492.0

* **Bug fix** IndexingMerger: protect form early "open store" exception [(Issue #2676)](https://github.com/FoundationDB/fdb-record-layer/issues/2676)
* **Performance**  LuceneOptimizedPostingsReader: skip checkIntegrity when dis-allowed  [(Issue #2666)](https://github.com/FoundationDB/fdb-record-layer/issues/2666)
* **Feature** Index Scrubber: convert "reset range" log message from info to debug [(Issue #2664)](https://github.com/FoundationDB/fdb-record-layer/issues/2664)
* **Feature** Record serializers now have a new default method that allows for validating bytes can be deserialized back to their original message [(Issue #2680)](https://github.com/FoundationDB/fdb-record-layer/issues/2680)

### 3.4.491.0

* **Feature** Agility Context: check/commit context before the user operation [(Issue #2626)](https://github.com/FoundationDB/fdb-record-layer/issues/2626)
* **Feature** AgilityContext: add applyInRecoveryPath test [(Issue #2636)](https://github.com/FoundationDB/fdb-record-layer/issues/2636)
* **Feature** Select Partition based on query filter [(Issue #2616)](https://github.com/FoundationDB/fdb-record-layer/issues/2616)

### 3.4.490.0


### 3.4.489.0

* **Bug fix** Equality index scan can match additional suffix starting with zero byte [(Issue #2650)](https://github.com/FoundationDB/fdb-record-layer/issues/2650)
* **Performance** Lucene: cleanup logs and lock release path [(Issue #2648)](https://github.com/FoundationDB/fdb-record-layer/issues/2648)

### 3.4.485.0

* **Bug fix** Improve logging for Lucene file locks [(Issue #2645)](https://github.com/FoundationDB/fdb-record-layer/issues/2645)
* **Bug fix** Review Lucene's use of executors [(Issue #2638)](https://github.com/FoundationDB/fdb-record-layer/issues/2638)

### 3.4.484.0

* **Bug fix** AgilityContext: Prevent operations after failed commit [(Issue #2642)](https://github.com/FoundationDB/fdb-record-layer/issues/2642)
* **Bug fix** LucenePartitioner.getNextNewerPartitionInfo() sometimes returns invalid (null) result [(Issue #2640)](https://github.com/FoundationDB/fdb-record-layer/issues/2640)

### 3.4.483.0

* **Bug fix** Log total records scanned and time during index build  [(Issue #2629)](https://github.com/FoundationDB/fdb-record-layer/issues/2629)
* **Bug fix** AgilityContext: prevent other threads writes after abort [(Issue #2634)](https://github.com/FoundationDB/fdb-record-layer/issues/2634)
* **Bug fix** Typobug: LUCENE_FILE_LOCK_TIME_WINDOW_MILLISECONDS has duplicated property key [(Issue #2632)](https://github.com/FoundationDB/fdb-record-layer/issues/2632)

### 3.4.482.0

* **Bug fix** IndexingMerger: assume a write-only context by using a non synchronized runner [(Issue #2611)](https://github.com/FoundationDB/fdb-record-layer/issues/2611)

### 3.4.481.0

* **Bug fix** AgilityContext.accept: release write lock in finally  [(Issue #2619)](https://github.com/FoundationDB/fdb-record-layer/issues/2619)
* **Bug fix** Commit after successful calls to AgilityContext.apply [(Issue #2618)](https://github.com/FoundationDB/fdb-record-layer/issues/2618)
* **Bug fix** Rename LuceneLogMessageKeys.PARTITION -> INDEX_PARTITION [(Issue #2614)](https://github.com/FoundationDB/fdb-record-layer/issues/2614)

### 3.4.480.0

* **Bug fix** Index Merge: Handle if the store fails to open [(Issue #2609)](https://github.com/FoundationDB/fdb-record-layer/issues/2609)

### 3.4.479.0

* **Bug fix** Record type key comparisons now can be used on synthetic types [(Issue #2587)](https://github.com/FoundationDB/fdb-record-layer/issues/2587)
* **Bug fix** Scan comparison ranges on the synthetic record type primary key do not break apart list key expression components [(Issue #2588)](https://github.com/FoundationDB/fdb-record-layer/issues/2588)
* **Breaking change** Union plans on synthetic types can choose different union comparison keys from before so continuations from plans from earlier builds may not be re-used on those queries [(Issue #2588)](https://github.com/FoundationDB/fdb-record-layer/issues/2588)

### 3.4.477.0


### 3.4.476.0

* **Bug fix** Lucene merge flush group/partition info AgileContext before executing a merge [(Issue #2605)](https://github.com/FoundationDB/fdb-record-layer/issues/2605)
* **Bug fix** Lucene merges no longer leave a corrupted index if it fails part way through  [(Issue #2600)](https://github.com/FoundationDB/fdb-record-layer/issues/2600)
* **Feature** Always defer merges during online indexing [(Issue #2602)](https://github.com/FoundationDB/fdb-record-layer/issues/2602)

### 3.4.475.0


### 3.4.474.0

* **Bug fix** Calls to AsyncUtil static methods should pass in an executor where possible [(Issue #2592)](https://github.com/FoundationDB/fdb-record-layer/issues/2592)
* **Bug fix** Lucene file lock may not be cleaned up in a failed merge [(Issue #2575)](https://github.com/FoundationDB/fdb-record-layer/issues/2575)

### 3.4.473.0

* **Bug fix** from is greater than to in a partition info [(Issue #2580)](https://github.com/FoundationDB/fdb-record-layer/issues/2580)
* **Bug fix** Log proper file count during listAllFiles [(Issue #2584)](https://github.com/FoundationDB/fdb-record-layer/issues/2584)
* **Bug fix** If an error happens during merge with an agile context, it may leak contexts [(Issue #2574)](https://github.com/FoundationDB/fdb-record-layer/issues/2574)

### 3.4.472.0

* **Performance** Lucene: avoid concurrent merges [(Issue #2570)](https://github.com/FoundationDB/fdb-record-layer/issues/2570)
* **Performance** Add metric for the size of list of files loaded by loadFileReferenceCacheForMemoization [(Issue #2566)](https://github.com/FoundationDB/fdb-record-layer/issues/2566)
* **Feature** Lucene: Merge periodically when rebalancing a lot of documents [(Issue #2567)](https://github.com/FoundationDB/fdb-record-layer/issues/2567)

### 3.4.471.0


### 3.4.469.0

* **Performance** Allow IN-union plans to be used to plan distinct queries with ordering constraints [(Issue #2556)](https://github.com/FoundationDB/fdb-record-layer/issues/2556)

### 3.4.468.0


### 3.4.467.0

* **Feature** IndexDeferredMaintenanceControl: Change the default to defer index operations [(Issue #2559)](https://github.com/FoundationDB/fdb-record-layer/issues/2559)

### 3.4.466.0

* **Bug fix** Revert [#2557](https://github.com/FoundationDB/fdb-record-layer/pull/2557) to avoid picking union ordering keys with duplicates [(Issue #2563)](https://github.com/FoundationDB/fdb-record-layer/issues/2563)
* **Bug fix** Use metrics instead of info log when rebalancing lucene partitions [(Issue #2509)](https://github.com/FoundationDB/fdb-record-layer/issues/2509)
* **Feature** Allow Non-Unique Lucene Partitioning Field Values [(Issue #2541)](https://github.com/FoundationDB/fdb-record-layer/issues/2541)

### 3.4.465.0

* **Bug fix** Avoid protobuf validation errors when expanding nested repeated index expressions [(Issue #2552)](https://github.com/FoundationDB/fdb-record-layer/issues/2552)
* **Performance** Allow IN-union plans to be used to plan distinct queries with ordering constraints [(Issue #2556)](https://github.com/FoundationDB/fdb-record-layer/issues/2556)
* **Feature** IndexingMerger: if merge fails, retry with a "skip rebalancing" flag [(Issue #2537)](https://github.com/FoundationDB/fdb-record-layer/issues/2537)

### 3.4.464.0


### 3.4.463.0


### 3.4.461.0

* **Bug fix** Permuted min and max indexes on repeated fields now index all entries if defined on a `FanOut` expression [(Issue #2543)](https://github.com/FoundationDB/fdb-record-layer/issues/2543)
* **Performance** Implement a proper copy-on-write `replace` for `TreeLike` [(Issue #2500)](https://github.com/FoundationDB/fdb-record-layer/issues/2500)
* **Feature** API for robust matching of `Value`s [(Issue #2499)](https://github.com/FoundationDB/fdb-record-layer/issues/2499)
* **Breaking change** Permuted min or max indexes on repeated fields need to be rebuilt to ensure completeness [(Issue #2543)](https://github.com/FoundationDB/fdb-record-layer/issues/2543)

### 3.4.459.0

* **Bug fix** The `PlanOrderingKey` can now handle multiple instances of the same expression appearing within the same key [(Issue #2452)](https://github.com/FoundationDB/fdb-record-layer/issues/2462)
* **Bug fix** The value portion of a `KeyWithValueExpression` no longer contributes to the `PlanOrderingKey` [(Issue #2469)](https://github.com/FoundationDB/fdb-record-layer/issues/2469)
* **Bug fix** Synthetic indexes built during `checkVersion` use the correct index maintainers. Adopters using synthetic records may need to rebuild or scrub older indexes [(Issue #2530)](https://github.com/FoundationDB/fdb-record-layer/issues/2530)
* **Feature** Support deleteRecordsWhere for some join indexes [(Issue #2532)](https://github.com/FoundationDB/fdb-record-layer/issues/2532)
* **Feature** Add an option for other non-time size events [(Issue #2525)](https://github.com/FoundationDB/fdb-record-layer/issues/2525)
* **Feature** FDBDirectoryLockFactory: Implement heartbeat during ensureValid [(Issue #2539)](https://github.com/FoundationDB/fdb-record-layer/issues/2539)
* **Feature** Harden Lucene continuations with respect to background work [(Issue #2523)](https://github.com/FoundationDB/fdb-record-layer/issues/2523)
* **Feature** FDBDirectoryLockFactory: better handling of lock's TTL [(Issue #2535)](https://github.com/FoundationDB/fdb-record-layer/issues/2535)
* **Feature** The planner will now select in-union plans in cases when the index contains additional columns that are not specified in the requested ordering [(Issue #2493)](https://github.com/FoundationDB/fdb-record-layer/issues/2493)

### 3.4.458.0

* **Bug fix** Coercion logic does not handle `Enum` type correctly [(Issue #2517)](https://github.com/FoundationDB/fdb-record-layer/issues/2517)

### 3.4.457.0

* **Bug fix** The index scrubbers now can handle value indexes on synthetic record types [(Issue #2513)](https://github.com/FoundationDB/fdb-record-layer/issues/2513)

### 3.4.456.0

* **Bug fix** OnlineIndexer.IndexingPolicy.toBuilder is missing some elements [(Issue #2521)](https://github.com/FoundationDB/fdb-record-layer/issues/2521)

### 3.4.455.0

* **Bug fix** Indexes on synthetic types are now included in `deleteRecordsWhere` operations [(Issue #2502)](https://github.com/FoundationDB/fdb-record-layer/issues/2502)
* **Bug fix** Use Locale.ROOT when parsing lucene queries [(Issue #2503)](https://github.com/FoundationDB/fdb-record-layer/issues/2503)
* **Feature** Unnested record types now support range deletion via `deleteRecordsWhere` [(Issue #2502)](https://github.com/FoundationDB/fdb-record-layer/issues/2502)
* **Feature** Disabled indexes no longer participate in `deleteRecordsWhere` operations [(Issue #2505)](https://github.com/FoundationDB/fdb-record-layer/issues/2505)
* **Breaking change** Unnested record types no longer produce synthetic records for empty repeated fields [(Issue #2512)](https://github.com/FoundationDB/fdb-record-layer/issues/2512)

### 3.4.454.0

* **Bug fix** Aggregate index match candidate does not name the grouping keys in the underlying select expression [(Issue #2506)](https://github.com/FoundationDB/fdb-record-layer/issues/2506)
* **Feature** Add map support for Maps in Lucene [(Issue #2488)](https://github.com/FoundationDB/fdb-record-layer/issues/2488)

### 3.4.453.0

* **Feature** Optimize insertion of older Lucene documents to avoid unnecessary partition re-balancing [(Issue #2494)](https://github.com/FoundationDB/fdb-record-layer/issues/2494)
* **Feature** Use multiple partitions to perform rebalancing & merge all partitions [(Issue #2491)](https://github.com/FoundationDB/fdb-record-layer/issues/2491)

### 3.4.452.0

* **Bug fix** Sync files referencing FieldInfosId [(Issue #2455)](https://github.com/FoundationDB/fdb-record-layer/issues/2455)
* **Bug fix** Support closing FDBIndexOutput [(Issue #2457)](https://github.com/FoundationDB/fdb-record-layer/issues/2457)
* **Bug fix** `FDBExceptions` now correctly wraps `InterruptedExceptions` instead of throwing an `IllegalArgumentException` [(Issue #2485)](https://github.com/FoundationDB/fdb-record-layer/issues/2485)
* **Bug fix** Fix topological sort instability [(Issue #2476)](https://github.com/FoundationDB/fdb-record-layer/pull/2476)
* **Performance** Cascades Pre-order Iterable is inefficient [(Issue #2481)](https://github.com/FoundationDB/fdb-record-layer/issues/2481)
* **Feature** Allow running Lucene's tests with a random seed & multiple iterations [(Issue #2479)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Support rearranging documents between Lucene partitions [(Issue #2465)](https://github.com/FoundationDB/fdb-record-layer/issues/2465)
* **Feature** The Cascades planner can now plan against aggregate indexes with repeated grouping columns [(Issue #2472)](https://github.com/FoundationDB/fdb-record-layer/issues/2472)
* **Feature** Online Indexing: add reverse order option [(Issue #2474)](https://github.com/FoundationDB/fdb-record-layer/issues/2474)

### 3.4.451.0

* **Bug fix** The `PlanOrderingKey` now can tolerate a field appearing in both the equality bound keys and the non-equality bound ones [(Issue #2462)](https://github.com/FoundationDB/fdb-record-layer/issues/2462)
* **Bug fix** Normalization of the plan ordering keys now avoids breaking up columns coming from a `ListKeyExpression` [(Issue #2463)](https://github.com/FoundationDB/fdb-record-layer/issues/2463)
* **Performance** Boolean normalization now handles (non-repeated) nested fields allowing for additional query plans to be matched [(Issue #2452)](https://github.com/FoundationDB/fdb-record-layer/issues/2452)
* **Feature** There is now a proto serialization of the `RecordQueryPlannerConfiguration` class to support planning with the same configuration with multiple JVMs [(Issue #2461)](https://github.com/FoundationDB/fdb-record-layer/issues/2461)

### 3.4.450.0

* **Feature** Support for Cross-Partition Queries [(Issue #2432)](https://github.com/FoundationDB/fdb-record-layer/issues/2432)
* **Feature** add feature to serialize/deserialize plans [(Issue #2443)](https://github.com/FoundationDB/fdb-record-layer/issues/2443)
* **Feature** IndexMerge should include key_space_path in logs [(Issue #2436)](https://github.com/FoundationDB/fdb-record-layer/issues/2436)
* **Breaking change** Support for the Protobuf 2 runtime has been removed from all artifacts [(Issue #1637)](https://github.com/FoundationDB/fdb-record-layer/issues/1637)

## 3.3

### Breaking Changes

The Guava dependency version has been updated to 31.1. Projects may need to check for compatibility with their own versions of the dependency, or they may need to consider using the shaded artifact if the new version is incompatible.


### 3.3.446.0


### 3.3.445.0

* **Bug fix** `OfTypeValue` does not work correctly with array types [(Issue #2440)](https://github.com/FoundationDB/fdb-record-layer/issues/2440)

### 3.3.444.0

* **Feature** Permuted min and max indexes are now available during planning and aggregate function execution [(Issue #2418)](https://github.com/FoundationDB/fdb-record-layer/issues/2418)
* **Feature** Add implementations of all sub-classes of `BaseIndexFileFormatTestCase` [(Issue #2421)](https://github.com/FoundationDB/fdb-record-layer/issues/2421)
* **Feature** Lucene merge with agility context: allow retry with diminished quotas upon failures [(Issue #2429)](https://github.com/FoundationDB/fdb-record-layer/issues/2429)

### 3.3.443.0

* **Bug fix** Fixe Unbalanced log info exception [(Issue #2392)](https://github.com/FoundationDB/fdb-record-layer/issues/2392)
* **Bug fix** Make stored fields test less flaky [(Issue #2401)](https://github.com/FoundationDB/fdb-record-layer/issues/2401)
* **Feature** Add lucene partitioning metadata [(Issue #2390)](https://github.com/FoundationDB/fdb-record-layer/issues/2390)
* **Feature** Lucene: cross transactional merge path - extra cleanup [(Issue #2415)](https://github.com/FoundationDB/fdb-record-layer/issues/2415)

### 3.3.442.0

* **Feature** FDBDirecotry: remove references to caller context to avoid future mis-usage [(Issue #2412)](https://github.com/FoundationDB/fdb-record-layer/issues/2412)
* **Feature** LuceneScaleTest: Add merge options to test configuration [(Issue #2410)](https://github.com/FoundationDB/fdb-record-layer/issues/2410)

### 3.3.441.0

* **Bug fix** To avoid breaking continuations during a deployment, the improvement to union key matching added in [3.3.348.0](#333480) is now guarded by a planner configuration flag [(Issue #2408)](https://github.com/FoundationDB/fdb-record-layer/issues/2408)

### 3.3.440.0

* **Feature** Lucene Property: Add a property to disable agility context [(Issue #2403)](https://github.com/FoundationDB/fdb-record-layer/issues/2403)

### 3.3.439.0

* **Bug fix** Fix Unbalanced log info exception [(Issue #2392)](https://github.com/FoundationDB/fdb-record-layer/issues/2392)
* **Performance** Support segment merging across transactions & larger segments [(Issue #2375)](https://github.com/FoundationDB/fdb-record-layer/issues/2375)

### 3.3.438.0

* **Performance** The old planner chooses a better plan ordering key for union comparisons to allow ordered union plans in more cases [(Issue #2336)](https://github.com/FoundationDB/fdb-record-layer/issues/2336)

### 3.3.437.0

* **Bug fix** `IndexAggregateFunctionCall.isGroupingPermutable` is now too liberal [(Issue #2388)](https://github.com/FoundationDB/fdb-record-layer/issues/2388)

### 3.3.436.0


### 3.3.435.0

* **Bug fix** Synthetic record type plans are now only generated if the type has at least one index that requires maintenance [(Issue #2377)](https://github.com/FoundationDB/fdb-record-layer/issues/2377)
* **Bug fix** Store file content on own file references [(Issue #2295)](https://github.com/FoundationDB/fdb-record-layer/issues/2295)
* **Bug fix** Store FieldInfo on file reference [(Issue #2284)](https://github.com/FoundationDB/fdb-record-layer/issues/2284)
* **Performance** Cache parsed FieldInfos in lucene index [(Issue #2374)](https://github.com/FoundationDB/fdb-record-layer/issues/2374)

### 3.3.433.0


### 3.3.432.0

* **Feature** Add exception info to the applicable IndexingMerger log messages [(Issue #2372)](https://github.com/FoundationDB/fdb-record-layer/issues/2372)

### 3.3.431.0


### 3.3.430.0

* **Bug fix** Fix `Column` hash calculation [(Issue #2369)](https://github.com/FoundationDB/fdb-record-layer/issues/2369)

### 3.3.429.0

* **Feature** indexMerge: perform the merging in multiple transaction [(Issue #2343)](https://github.com/FoundationDB/fdb-record-layer/issues/2343)

### 3.3.428.0

* **Bug fix** Convert FDBDirectory.writeData to a regular func [(Issue #2351)](https://github.com/FoundationDB/fdb-record-layer/issues/2351)

### 3.3.427.0

* **Bug fix** Fix BufferOverflowException triggered by FDBIndexOutput [(Issue #2345)](https://github.com/FoundationDB/fdb-record-layer/issues/2345)
* **Feature** Introduce the synthetic record type `UnnestedRecordType` to allow for more expressive indexes on nested messages [(Issue #2313)](https://github.com/FoundationDB/fdb-record-layer/issues/2313)

### 3.3.426.0


### 3.3.425.0

* **Feature** IndexAggregateFunctionCall.extractEqualityBoundFields does not recognize one-of components [(Issue #2341)](https://github.com/FoundationDB/fdb-record-layer/issues/2341)

### 3.3.424.0

* **Bug fix** desensitize plan hash computations from output types [(Issue #2332)](https://github.com/FoundationDB/fdb-record-layer/issues/2332)
* **Performance** clean up computation of plan hashes [(Issue #2346)](https://github.com/FoundationDB/fdb-record-layer/issues/2346)

### 3.3.423.0

* **Bug fix** RecordQueryAbstractDataModificationPlan does not produce stable hash with CoercionTrieNode [(Issue #2330)](https://github.com/FoundationDB/fdb-record-layer/issues/2330)
* **Bug fix** mutateRecord in RecordQueryAbstractDataModificationPlan fails when underlying metadata ref changes. [(Issue #2337)](https://github.com/FoundationDB/fdb-record-layer/issues/2337)

### 3.3.422.0

* **Performance** secondary node slot index for r-trees [(Issue #2215)](https://github.com/FoundationDB/fdb-record-layer/issues/2215)
* **Feature** Lucene: add metric to measure Lucene's find merges [(Issue #2333)](https://github.com/FoundationDB/fdb-record-layer/issues/2333)

### 3.3.421.0

* **Bug fix** Set the UPDATE's PipelineSize to 1 [(Issue #2324)](https://github.com/FoundationDB/fdb-record-layer/issues/2324)

### 3.3.420.0


### 3.3.419.0


### 3.3.418.0

* **Bug fix** Fix hash code instability in `RecordQueryAbstractDataModificationPlan` [(Issue #2325)](https://github.com/FoundationDB/fdb-record-layer/issues/2325)
* **Bug fix** Rethrow original IOExceptions exceptions in lucene code [(Issue #2293)](https://github.com/FoundationDB/fdb-record-layer/issues/2293)
* **Bug fix** Add proper clone to LuceneStoredFieldsReader [(Issue #2297)](https://github.com/FoundationDB/fdb-record-layer/issues/2297)
* **Performance** Lucene performance: avoid merge calculation before deferring merge [(Issue #2319)](https://github.com/FoundationDB/fdb-record-layer/issues/2319
* **Feature** LuceneLoggerInfoStream should check if trace is enabled in isEnabled [(Issue #2317)](https://github.com/FoundationDB/fdb-record-layer/issues/2317)

### 3.3.417.0


### 3.3.416.0

* **Feature** planner support for multidimensional indexes [(Issue #2305)](https://github.com/FoundationDB/fdb-record-layer/issues/2305)

### 3.3.415.0

* **Performance** Lucene: avoid segments merge at closing [(Issue #2279)](https://github.com/FoundationDB/fdb-record-layer/issues/2279)

### 3.3.414.0


### 3.3.413.0

* **Feature** Support Java UDFs in Cascades [(Issue #2307)](https://github.com/FoundationDB/fdb-record-layer/issues/2307)

### 3.3.412.0

* **Bug fix** Throw RecordCoreException when missing FileReference [(Issue #2291)](https://github.com/FoundationDB/fdb-record-layer/issues/2291)
* **Bug fix** Add cause to LuceneRecordCursor "Failed to get document" exception [(Issue #2283)](https://github.com/FoundationDB/fdb-record-layer/issues/2283)

### 3.3.411.0


### 3.3.410.0

* **Bug fix** segregate different types of lucene querying [(Issue #2276)](https://github.com/FoundationDB/fdb-record-layer/issues/2276)

### 3.3.409.0

* **Bug fix** Fix coerceArray to work with list of primitives [(Issue #2274)](https://github.com/FoundationDB/fdb-record-layer/issues/2274)

### 3.3.407.0

* **Feature** Provide support for empty list in InOpValue [(Issue #2270)](https://github.com/FoundationDB/fdb-record-layer/issues/2270)

### 3.3.406.0

* **Performance** Remove unnecessary supplyAsync from FDBDirectory.writeSchema [(Issue #2237)](https://github.com/FoundationDB/fdb-record-layer/issues/2237)

### 3.3.405.0

* **Performance** Indexing throttle: reduce limit faster during repeating failures [(Issue #2240)](https://github.com/FoundationDB/fdb-record-layer/issues/2240)

### 3.3.404.0

* **Feature** multidimensional index maintainer [(Issue #2215)](https://github.com/FoundationDB/fdb-record-layer/issues/2215)

### 3.3.403.0


### 3.3.402.0

* **Bug fix** COALESCE does not work with non-scalar types [(Issue #2242)](https://github.com/FoundationDB/fdb-record-layer/issues/2242)
* **Bug fix** Change log level of "found lucene analyzer" to debug [(Issue #2250)](https://github.com/FoundationDB/fdb-record-layer/issues/2250)
* **Feature** Re-word misleading error message "unable to build index [(Issue #2247)](https://github.com/FoundationDB/fdb-record-layer/issues/2247)

### 3.3.401.0


### 3.3.400.0

* **Bug fix** Resolve Lucene readSchema deadlock [(Issue #2234)](https://github.com/FoundationDB/fdb-record-layer/issues/2234)

### 3.3.399.0


### 3.3.398.0

* **Bug fix** Splitting disjunction into union produces duplicates [(Issue #2230)](https://github.com/FoundationDB/fdb-record-layer/issues/2230)

### 3.3.397.0

* **Performance** Lucene: allow skipping checkIntegrity for all PostingsFormat inheritors [(Issue #2224)](https://github.com/FoundationDB/fdb-record-layer/issues/2224)

### 3.3.396.0

* **Feature** OnlineIndexer: IndexingThrottle: re-write limit handling  [(Issue #2106)](https://github.com/FoundationDB/fdb-record-layer/issues/2106)

### 3.3.395.0

* **Bug fix** Lucene: allow fieldsProducer close (and commit) only if it was initialized [(Issue #2222)](https://github.com/FoundationDB/fdb-record-layer/issues/2222)
* **Performance** a wider index should be preferred over a narrower index if choosing an index scan over the wider index avoids a fetch [(Issue #2218)](https://github.com/FoundationDB/fdb-record-layer/issues/2218)
* **Feature** Stores can now be configured to update their state cacheability during store opening or creation [(Issue #2203)](https://github.com/FoundationDB/fdb-record-layer/issues/2203)

### 3.3.394.0


### 3.3.393.0

* **Feature** Add more metrics to Lucene operations [(Issue #2210)](https://github.com/FoundationDB/fdb-record-layer/issues/2210)

### 3.3.392.0

* **Feature** Support Greatest and Least scalar functions [(Issue #2206)](https://github.com/FoundationDB/fdb-record-layer/issues/2206)
* **Feature** Support coalesce scalar function [(Issue #2206)](https://github.com/FoundationDB/fdb-record-layer/issues/2207)


### 3.3.391.0


### 3.3.390.0

* **Feature** Lucene: add context property to skip data integrity check [(Issue #2198)](https://github.com/FoundationDB/fdb-record-layer/issues/2198)

### 3.3.389.0

* **Feature** Add a way to relax stop qord queries [(Issue #2196)](https://github.com/FoundationDB/fdb-record-layer/issues/2196)

### 3.3.388.0

* **Bug fix** Parameterized dual-planner test results do not show test name [(Issue #2201)](https://github.com/FoundationDB/fdb-record-layer/issues/2201)
* **Feature** Lucene: add context property to skip data integrity check [(Issue #2198)](https://github.com/FoundationDB/fdb-record-layer/issues/2198)

### 3.3.387.0


### 3.3.386.0


### 3.3.386.0

* **Bug fix** Lifting Java type to Cascades type is inconsistent [(Issue #2176)](https://github.com/FoundationDB/fdb-record-layer/issues/2176)

### 3.3.385.0


### 3.3.384.0


### 3.3.382.0

* **Bug fix** Fix grouping requested order if it is empty [(Issue #2150)](https://github.com/FoundationDB/fdb-record-layer/issues/2150)

### 3.3.381.0

* **Performance** only break ORs into UNIONs if beneficial for index matching [(Issue #2147)](https://github.com/FoundationDB/fdb-record-layer/issues/2147)

### 3.3.380.0

* **Bug fix** flatmap should honor skips and limits [(Issue #2140)](https://github.com/FoundationDB/fdb-record-layer/issues/2140)

### 3.3.379.0

* **Feature** cascades planner needs to enumerate OR factorizations properly [(Issue #2131)](https://github.com/FoundationDB/fdb-record-layer/issues/2131)

### 3.3.378.0

* **Bug fix** Removed infinite recursion from `LuceneIndexQueryPlan`'s `toString` implementation [(Issue #2136)](https://github.com/FoundationDB/fdb-record-layer/issues/2136)
* **Feature** Record versions are now queryable by the Cascades planner, including mathcing on version indexes [(Issue #2089)](https://github.com/FoundationDB/fdb-record-layer/issues/2089)

### 3.3.377.0

* **Feature** Allow placeholders in aggregate index match candidate's underlying `SelectExpression` [(Issue #2135)](https://github.com/FoundationDB/fdb-record-layer/issues/2135)

### 3.3.376.0

* **Bug fix** The plan string for `RecordQueryMapPlan` is reverted back to its value before [3.3.375.0](#333750) [(Issue #2132)](https://github.com/FoundationDB/fdb-record-layer/issues/2132)

### 3.3.375.0

* **Bug fix** Filtered match candidate is incorrectly considered for unfiltered scan [(Issue #2118)](https://github.com/FoundationDB/fdb-record-layer/issues/2118)
* **Feature** The `PlanStringRepresentation` class separates out plan explain printing from `RecordQueryPlan::toString` including adding logic to create length-limited strings [(Issue #2112)](https://github.com/FoundationDB/fdb-record-layer/issues/2112)

### 3.3.374.0

* **Feature** Add throttling tags to context config [(Issue #2115)](https://github.com/FoundationDB/fdb-record-layer/issues/2115)
* **Feature** Add optional conflict tracking to context config [(Issue #2116)](https://github.com/FoundationDB/fdb-record-layer/issues/2116)

### 3.3.373.0

* **Feature** Refactor KeySpaceTreeResolver out of KeySpaceCountTree [(Issue #2121)](https://github.com/FoundationDB/fdb-record-layer/issues/2121)

### 3.3.372.0

* **Bug fix** FDBRecordStore: keep userVersionChecker during copyFrom store [(Issue #2109)](https://github.com/FoundationDB/fdb-record-layer/issues/2109)

### 3.3.371.0

* **Feature** Use Index.predicate as an index maintenance filter when appropriate [(Issue #2069)](https://github.com/FoundationDB/fdb-record-layer/issues/2069)

### 3.3.370.0

* **Bug fix** Captured constraints of filtered indexes are too restrictive [(Issue #2104)](https://github.com/FoundationDB/fdb-record-layer/issues/2104)

### 3.3.369.0

* **Bug fix** Restore a removed method from `CascadesPlanner` and mark it for deprecation [(Issue #2102)](https://github.com/FoundationDB/fdb-record-layer/issues/2102)

### 3.3.368.0

* **Bug fix** A bug in mutual indexing may cause low performance [(Issue #2100)](https://github.com/FoundationDB/fdb-record-layer/issues/2100)

### 3.3.367.0

* **Feature** Literal extraction and plan constraints in Cascades [(Issue #2094)](https://github.com/FoundationDB/fdb-record-layer/issues/2094)

### 3.3.366.0

* **Bug fix** Don't write to disabled Synthetic Indexes [(Issue #2099)](https://github.com/FoundationDB/fdb-record-layer/issues/2099)
* **Bug fix** Add support for `int32` in record metadata `Value` [(Issue #2091)](https://github.com/FoundationDB/fdb-record-layer/issues/2091)

### 3.3.365.0

* **Performance** Mutual indexing - better handle final iteration conflicts [(Issue #2082)](https://github.com/FoundationDB/fdb-record-layer/issues/2082)
* **Feature** The `LocatableResolver` interface has been enhanced with read-only and transaction-scoped APIs [(Issue #2062)](https://github.com/FoundationDB/fdb-record-layer/issues/2062)
* **Feature** Indexing stamp operations: return the post change values [(Issue #2080)](https://github.com/FoundationDB/fdb-record-layer/issues/2080)

### 3.3.364.0

* **Feature** Remove old Lucene auto-complete functionality and create a proper composable query clause for auto-complete functionality [(Issue #2083)](https://github.com/FoundationDB/fdb-record-layer/issues/2083)
* **Breaking change** Remove old Lucene auto-complete functionality and deprecated auto-complete-related indexing options [(Issue #2083)](https://github.com/FoundationDB/fdb-record-layer/issues/2083)

### 3.3.363.0

* **Bug fix** Improve covering field path computation for the case where a normalized field still takes up more than position [(Issue #2085)](https://github.com/FoundationDB/fdb-record-layer/issues/2085)

### 3.3.362.0

* **Bug fix** Fixes correcting issues introduced in #2058 [(Issue #2061)](https://github.com/FoundationDB/fdb-record-layer/issues/2061)

### 3.3.361.0


### 3.3.360.0

* **Feature** Feature 1 [(Issue #2071)](https://github.com/FoundationDB/fdb-record-layer/issues/2071)

### 3.3.359.0


### 3.3.358.0

* **Feature** improve join enumeration and plan memoization [(Issue #2061)](https://github.com/FoundationDB/fdb-record-layer/issues/2061)

### 3.3.357.0

* **Bug fix** Make testMutualIndexingBlocker less volatile [(Issue #2066)](https://github.com/FoundationDB/fdb-record-layer/issues/2066)
* **Feature** mutual indexing - add fragment information to throttle logs [(Issue #2059)](https://github.com/FoundationDB/fdb-record-layer/issues/2059)

### 3.3.356.0

* **Feature** Pull up getRecordState to the FDBRecordStoreBase [(Issue #2065)](https://github.com/FoundationDB/fdb-record-layer/issues/2065)


### 3.3.355.0

* **Performance** An unnecessary recount of records on new stores with a record count key has been removed [(Issue #2052)](https://github.com/FoundationDB/fdb-record-layer/issues/2052)

### 3.3.353.0


### 3.3.352.0


### 3.3.351.0
* **Feature** Supports bitset_contains in Lucene query


### 3.3.350.0


### 3.3.349.0

* **Bug fix** The `missingRange`s method in `RangeSet`s now adds more precise read conflict ranges to avoid introducing unneccessary conflicts [(Issue #2024)](https://github.com/FoundationDB/fdb-record-layer/issues/2024)

### 3.3.348.0


### 3.3.347.0


### 3.3.346.0


### 3.3.345.0


### 3.3.344.0

* **Bug fix** resolves #2035: lucene index auto-complete should adhere to limits as well as should populate primary key info correctly [(Issue #2035)](https://github.com/FoundationDB/fdb-record-layer/issues/2035)

### 3.3.343.0

* **Feature** refactor lucene index auto complete and spell check [(Issue #2035)](https://github.com/FoundationDB/fdb-record-layer/issues/2035)

### 3.3.342.0


### 3.3.341.0


### 3.3.340.0

* **Bug fix** Cascades planner no longer trips over `VALUE` indexes which contain all fields in the primary key [(Issue #2015)](https://github.com/FoundationDB/fdb-record-layer/issues/2015)

### 3.3.339.0

* **Feature** Make Index.options immutable. This replaces the internal data structure of Index.options with an immutable map so that a user cannot call getOptions().put()[(Issue #2016)](https://github.com/FoundationDB/fdb-record-layer/issues/2016)
* **Feature** Online Indexing: Add transaction time quota and initial scanned records limit [(Issue #2013)](https://github.com/FoundationDB/fdb-record-layer/issues/2013)

### 3.3.338.0

* **Bug fix** OnlineIndexer: make stamp operations public [(Issue #2027)](https://github.com/FoundationDB/fdb-record-layer/issues/2027)

### 3.3.337.0


### 3.3.336.0

* **Feature** improve logic to push plans through a fetch [(Issue #2020)](https://github.com/FoundationDB/fdb-record-layer/issues/2020)

### 3.3.335.0

* **Bug fix** The `StoreSubTimer` class now forwards metrics to an underlying timer when `add` is called, which previously could cause metrics reported only on commit to disappear entirely [(Issue #2010)](https://github.com/FoundationDB/fdb-record-layer/issues/2010)

### 3.3.334.0

* **Bug fix** throttleDelayAndMaybeLogProgress causes NullPointerException [(Issue #2006)](https://github.com/FoundationDB/fdb-record-layer/issues/2006)
* **Feature** Sparse index support in Cascades [(Issue #1997)](https://github.com/FoundationDB/fdb-record-layer/issues/1997)

### 3.3.333.0

* **Feature** Indexing typestamp: add query + flow control [(Issue #1980)](https://github.com/FoundationDB/fdb-record-layer/issues/1980)

### 3.3.332.0

* **Bug fix** The `EMPTY_SCANS` metric no longer double counts empty scans that call `hasNext` multiple times [(Issue #2002)](https://github.com/FoundationDB/fdb-record-layer/issues/2002)
* **Performance** Additional instrumentaiton of the `RangeSet` is added to account for time spent during index builds  [(Issue #1995)](https://github.com/FoundationDB/fdb-record-layer/issues/1995)
* **Performance** Metrics that are dependent on transaction commit will only be recorded after its associated transaction is successfully committed [(Issue #1998)](https://github.com/FoundationDB/fdb-record-layer/issues/1998)

### 3.3.331.0

* **Performance** Non-unique indexes no longer read or clear the uniqueness violation space during maintenance [(Issue #1973)](https://github.com/FoundationDB/fdb-record-layer/issues/1973)
* **Performance** Adds store timer metrics to the indexer progress metrics message [(Issue #1984)](https://github.com/FoundationDB/fdb-record-layer/issues/1984)
* **Performance** Instrumentation is added for delays inserted during retries and index builds [(Issue #1993)](https://github.com/FoundationDB/fdb-record-layer/issues/1993)

### 3.3.330.0

* **Feature** planner should only attempt in-to-join if plan uses correlated predicate as a search argument for a scan [(Issue #1985)](https://github.com/FoundationDB/fdb-record-layer/issues/1985)

### 3.3.329.0

* **Performance** Default value of `LOAD_RECORDS_VIA_GETS` property reverted to `false` to match behavior prior to [3.3.327.0](#333270) [(Issue #1982)](https://github.com/FoundationDB/fdb-record-layer/issues/1982)

### 3.3.328.0

* **Bug fix** Cascades Aggregate index plan matching ignores predicates [(Issue #1974)](https://github.com/FoundationDB/fdb-record-layer/issues/1974)

### 3.3.327.0

* **Performance** Add optional mechanism for loading records via single-key gets instead of a scan [(Issue #1967)](https://github.com/FoundationDB/fdb-record-layer/issues/1967)

### 3.3.326.0

* **Performance** A new counter, `EMPTY_SCANS`, tracks how many empty range reads are executed [(Issue #1970)](https://github.com/FoundationDB/fdb-record-layer/issues/1970)

### 3.3.325.0

* **Feature** Refactor KeyValueCursor to allow extension [(Issue #1957)](https://github.com/FoundationDB/fdb-record-layer/issues/1957)
* **Feature** Expose version change info from FDBRecordStore [(Issue #1965)](https://github.com/FoundationDB/fdb-record-layer/issues/1965)
* **Feature** Mutual concurrent indexing [(Issue #1853)](https://github.com/FoundationDB/fdb-record-layer/issues/1853)
* **Feature** Indexing: Throw PartlyBuiltException when appropriate [(Issue #1961)](https://github.com/FoundationDB/fdb-record-layer/issues/1961)

### 3.3.324.0

* **Bug fix** fix bug to allow non-covering lucene index scans over synthetic records [(Issue #1959)](https://github.com/FoundationDB/fdb-record-layer/issues/1959)

### 3.3.323.0


### 3.3.322.0

* **Feature** Support for IN predicates in Cascades planner [(Issue #1955)](https://github.com/FoundationDB/fdb-record-layer/issues/1955)

### 3.3.321.0


### 3.3.320.0


### 3.3.319.0


### 3.3.318.0


### 3.3.317.0

* **Bug fix** Refactor ThenKeyExpression#normalizeKeyForPositions to reduce hotspot detected by removing stream() implementation to reduce new object creation [(Issue #1940)](https://github.com/FoundationDB/fdb-record-layer/issues/1940)
* **Performance** Unrolled record deletes (introduced in [3.3.309.0](#333090) are now on by default [(Issue #1942)](https://github.com/FoundationDB/fdb-record-layer/issues/1942)

### 3.3.316.0


### 3.3.315.0

* **Bug fix** Fix byte counting bug for remote fetch [(Issue #1934)](https://github.com/FoundationDB/fdb-record-layer/issues/1934)
* **Performance** Reduce the number of extraneous clear ranges issued during `checkVersion` [(Issue #1936)](https://github.com/FoundationDB/fdb-record-layer/issues/1936)

### 3.3.314.0

* **Bug fix** Indexing: periodic "Built Range" log message is broken [(Issue #1898)](https://github.com/FoundationDB/fdb-record-layer/issues/1898)

### 3.3.313.0

* **Performance** The number of array copies when constructing continuations should be decreased by relying more on `ByteString` internally [(Issue #1923)](https://github.com/FoundationDB/fdb-record-layer/issues/1923)

### 3.3.312.0

* **Bug fix** Fix covering plan for a Lucene index over a Synthetic Record Type [(Issue #1927)](https://github.com/FoundationDB/fdb-record-layer/issues/1927)

### 3.3.311.0

* **Bug fix** Sorting and comparing nested fields get confused [(Issue #1907)](https://github.com/FoundationDB/fdb-record-layer/issues/1907)

### 3.3.310.0


### 3.3.309.0

* **Feature** Non-idempotent target indexes can now be built from an existing index [(Issue #1430)](https://github.com/FoundationDB/fdb-record-layer/issues/1430)

### 3.3.307.0


### 3.3.306.0

* **Feature** Support additional streaming modes: LARGE, MEDIUM, SMALL [(Issue ##1915)](https://github.com/FoundationDB/fdb-record-layer/issues/#1915)

### 3.3.305.0


### 3.3.304.0

* **Feature** Lucene search with highlighting the terms [(Issue #1862)](https://github.com/FoundationDB/fdb-record-layer/issues/1862)

### 3.3.303.0

* **Bug** Pulling up a Value through Ordering is not entirely correct [(Issue #1905)](https://github.com/FoundationDB/fdb-record-layer/issues/1905)
* **Feature** Support planning aggregate indexes in Cascades. [(Issue #1885)](https://github.com/FoundationDB/fdb-record-layer/issues/1885)

### 3.3.302.0

* **Bug fix** ensure that partial records are created as proper protobuf messages [(Issue #1893)](https://github.com/FoundationDB/fdb-record-layer/issues/1893)

### 3.3.301.0

* **Bug fix** Account for FDBStoreTimer.Counts.BYTES_READ in remote fetch [(Issue #1889)](https://github.com/FoundationDB/fdb-record-layer/issues/1889)
* **Bug fix** The `RecordQueryPlanner` now chooses more efficient indexes for and-queries with one-of-them predicates on repeated nested fields [(Issue #1876)](https://github.com/FoundationDB/fdb-record-layer/issues/1876)
* **Performance** Single record deletes can now be configured to be executed as multiple single-key clears instead of one range clear, which should be an improvement for LSM-based FDB storage engines [(Issue #1493)](https://github.com/FoundationDB/fdb-record-layer/issues/1493)

### 3.3.300.0


### 3.3.298.0

* **Feature** Add a method to construct a single record from a raw indexed record [(Issue #1865)](https://github.com/FoundationDB/fdb-record-layer/issues/1865)
* **Feature** Joined record types now allow some kinds of function key expressions in their join conditions [(Issue #1872)](https://github.com/FoundationDB/fdb-record-layer/issues/1872)
* **Feature** Allow users to name enum types in `TypeRepository` [(Issue #1847)](https://github.com/FoundationDB/fdb-record-layer/issues/1847)
* **Feature** Support planning of (covering) index scans, fetches over synthetic record types [(Issue #1886)](https://github.com/FoundationDB/fdb-record-layer/issues/1886)

### 3.3.297.0

* **Bug fix** `TimeWindowLeaderboardWindowUpdate` `rebuild` = `NEVER` is not honored for new directory [(Issue #1878)](https://github.com/FoundationDB/fdb-record-layer/issues/1878)

### 3.3.296.0

* **Performance** Looking up logical values from `DirectoryLayerDirectory`s no longer needs to create new transactions [(Issue #1857)](https://github.com/FoundationDB/fdb-record-layer/issues/1857)

### 3.3.295.0

* **Breaking change** Enable incremental builds. Upgrade guava dependency to 31.1-jre. [(Issue #1868)](https://github.com/FoundationDB/fdb-record-layer/issues/1868)

## 3.2

### Features

This version of the Record Layer allows the FDB API version to be configured through the `FDBDatabaseFactory`. This means that while this version allows the client to be configured to use 7.1 features, it also supports connecting to 6.3 FDB clusters if the API version is set appropriately. Note that setting the API version does restrict the set of potential FDB server versions that can be connected to, so this configuration change should only be made if the FDB server has already been updated.
 
New index state "READABLE_UNIQUE_PENDING" - the proper way to roll this feature out is: 
1. The adopter should upgrade to the new Record Layer version and deploy the version everywhere.
2. The format version should be set READABLE_UNIQUE_PENDING_FORMAT_VERSION.
3. Only after all the possible clients are upgraded to support the new state, the adopter may set the allowPendingState on the indexing policy of new index builds. 
An index may be in this new state if it is fully built, the unique flag is set, and duplications were found during online indexing. From the code point of view, it is defined as scannable but not readable.  


### Breaking Changes

The FoundationDB Java binding dependency has been updated to 7.1 with this release. This means that clients also need to update their main FDB C client to a 7.1 version. Adopters that still wish to connect to an FDB cluster running a 6.3 or 7.0 server version can do so by packaging additional FDB C clients at the appropriate version(s) using the [FDB multi-version client feature](https://apple.github.io/foundationdb/api-general.html#multi-version-client).

This release also updates downstream dependency versions. Most notably, the proto3 artifacts now require Protobuf version 3.20.1.

### 3.2.293.0


### 3.2.292.0

* **Bug fix** incorrect assertion of PK length fails remote fetch with fallback [(Issue #1845)](https://github.com/FoundationDB/fdb-record-layer/issues/1845)

### 3.2.291.0

* **Feature** Support for remote fetch when common primary key is unavailable. This changes the (experimental) API in a non-backwards-compatible way - the common-primary-key is no longer required and can be calculated internally [(Issue #1805)](https://github.com/FoundationDB/fdb-record-layer/issues/1805)
* **Feature** Merge online indexer sub modules - "by records" and 'multi target by records" [(Issue #1432)](https://github.com/FoundationDB/fdb-record-layer/issues/1432)
* **Feature** Introduce JMH framework [(Issue #1157)](https://github.com/FoundationDB/fdb-record-layer/issues/1157)

### 3.2.290.0


### 3.2.289.0


### 3.2.288.0

* **Bug fix** Lower the severity of error when logging incompatible index for remote fetch [(Issue #1828)](https://github.com/FoundationDB/fdb-record-layer/issues/1828)
* **Feature** Make `FDBRecordContext` remember its config [(Issue #1830)](https://github.com/FoundationDB/fdb-record-layer/issues/1830)

### 3.2.287.0

* **Feature** Use different analyzers for different fields in one Lucene index, and enable exclusion of fields for auto-complete [(Issue #1824)](https://github.com/FoundationDB/fdb-record-layer/issues/1824)

### 3.2.286.0

* **Feature** Cascades planning of logical group by [(Issue #1791)](https://github.com/FoundationDB/fdb-record-layer/issues/1791)

### 3.2.285.0

* **Bug fix** Fix flaky test by allowing larger than expected scans [(Issue #1788)](https://github.com/FoundationDB/fdb-record-layer/issues/1788)
* **Performance** Reverse directory lookups can now borrow a read version from another transaction to avoid GRV costs [(Issue #1714)](https://github.com/FoundationDB/fdb-record-layer/issues/1714)
* **Feature** Query returns null for unset non-repeated fields [(Issue #1718)](https://github.com/FoundationDB/fdb-record-layer/issues/1718)
* **Feature** Emit a different KPI for each reason of index rebuilds [(Issue #1798)](https://github.com/FoundationDB/fdb-record-layer/issues/1798)
* **Feature** Trigger a log message for every index state change [(Issue #1809)](https://github.com/FoundationDB/fdb-record-layer/issues/1809)
* **Feature** Support READABLE_UNIQUE_PENDING IndexState [(Issue #1611)](https://github.com/FoundationDB/fdb-record-layer/issues/1611)

### 3.2.284.0

* **Bug fix** Update client event parsing to handle tenant [(Issue #1802)](https://github.com/FoundationDB/fdb-record-layer/issues/1802)

### 3.2.283.0


### 3.2.282.0

* **Performance** A new index scan type variant has been added that make better use of existing caches in repeated executions of the same query [(Issue #1758)](https://github.com/FoundationDB/fdb-record-layer/issues/1758)

### 3.2.281.0

* **Bug fix** Add log for fallback triggered [(Issue #1782)](https://github.com/FoundationDB/fdb-record-layer/issues/1782)

### 3.2.280.0


### 3.2.279.0


### 3.2.278.0


### 3.2.277.0


### 3.2.276.0


### 3.2.275.0

* **Bug fix** Fix fallback for scanIndexRecords [(Issue ##1761)](https://github.com/FoundationDB/fdb-record-layer/issues/#1761)

### 3.2.273.0


### 3.2.272.0

* **Bug fix** Proximity search does not works well when the term has multi-word synonyms [(Issue #1752)](https://github.com/FoundationDB/fdb-record-layer/issues/1752)
* **Feature** Index Remote Fetch for index scan methods [(Issue #1751)](https://github.com/FoundationDB/fdb-record-layer/issues/1751)

### 3.2.271.0

* **Bug fix** Lucene auto-complete should apply the limit to the result cursor [(Issue #1745)](https://github.com/FoundationDB/fdb-record-layer/issues/1745)

### 3.2.270.0

* **Bug fix** FDB Java dependency version updated to 7.1.10 to incorporate fixes over older 7.1 versions [(Issue #1743)](https://github.com/FoundationDB/fdb-record-layer/issues/1743)
* **Feature** Use Lucene query component to pass in the parameter for auto-complete's highlighting option [(Issue #1740)](https://github.com/FoundationDB/fdb-record-layer/issues/1740)

### 3.2.269.0


### 3.2.268.0


### 3.2.267.0

* **Feature** Online Indexer: allow fallback to by-record if the previous stamp is multi-target [(Issue #1736)](https://github.com/FoundationDB/fdb-record-layer/issues/1736)

### 3.2.266.0

* **Bug fix** Fix double counting of limits in remote fetch [(Issue #1730)](https://github.com/FoundationDB/fdb-record-layer/issues/1730)
* **Feature** Improve remote fetch fallback  to allow fallback after a few records have been returned [(Issue #1727)](https://github.com/FoundationDB/fdb-record-layer/issues/1727)
* **Feature** API version for tests changed to 7.1 [(Issue #1734)](https://github.com/FoundationDB/fdb-record-layer/issues/1734)

### 3.2.265.0

* **Bug fix** Cascades does not reconstruct a KeyWithValue correctly [(Issue #1725)](https://github.com/FoundationDB/fdb-record-layer/issues/1725)

### 3.2.264.0

* **Feature** Publish test jar as part of the regular distribution [(Issue #1703)](https://github.com/FoundationDB/fdb-record-layer/issues/1703)
* **Feature** UserVersionChecker should be getting RecordMetaDataProto.DataStoreInfo [(Issue #1710)](https://github.com/FoundationDB/fdb-record-layer/issues/1710)

### 3.2.262.0

* **Bug fix** Failed no-ops no longer log at `ERROR` [(Issue #1692)](https://github.com/FoundationDB/fdb-record-layer/issues/1692)
* **Performance** Lucene auto-complete is now handled by running queries on the main index to allow it to avoid needing a separate directory [(Issue #1682)](https://github.com/FoundationDB/fdb-record-layer/issues/1682)
* **Feature** Cursors now can map updates to their continuations via the `.mapContinuation` method [(Issue #1663)](https://github.com/FoundationDB/fdb-record-layer/issues/1663)

### 3.2.261.0

* **Performance** Lucene merge based on probability [(Issue #1676)](https://github.com/FoundationDB/fdb-record-layer/issues/1676)

### 3.2.260.0


### 3.2.259.0

* **Bug fix** Synchronize TransactionalRunner.close [(Issue #1659)](https://github.com/FoundationDB/fdb-record-layer/issues/1659)
* **Feature** The FDB API version can now be configured through the `FDBDatabaseFactory` [(Issue #1639)](https://github.com/FoundationDB/fdb-record-layer/issues/1639)
* **Feature** Match index hints in a query [(Issue #1671)](https://github.com/FoundationDB/fdb-record-layer/issues/1671)
* **Feature** Remote Fetch feature using FDB's getMappedRange new API [(Issue #1560)](https://github.com/FoundationDB/fdb-record-layer/issues/1560)
* **Breaking change** Extenders of StandardIndexMaintainers will inherit the scanRemoteFetch implementation. If this is not suppoted for the subclass, override the method [(Issue #1560)](https://github.com/FoundationDB/fdb-record-layer/issues/1560)

### 3.2.258.0

* **Bug fix** FDBRecordContext::newRunner drops property storage [(Issue #1645)](https://github.com/FoundationDB/fdb-record-layer/issues/1645)

### 3.2.257.0

* **Breaking change** The FDB dependency has been updated to require version 7.1 [(Issue #1636)](https://github.com/FoundationDB/fdb-record-layer/issues/1636)
* **Breaking change** Additional downstream dependency versions have been upgraded, including Protobuf [(Issue #1640)](https://github.com/FoundationDB/fdb-record-layer/issues/1640)

## 3.1

### Breaking Changes

This version of the Record Layer changes the Java source and target compatibility to version 11.  Downstream projects need to be aware that the byte code produced is of class file version `55.0` going forward.

### 3.1.256.0

* **Feature** Create a transactional runner that does not retry [(Issue #1615)](https://github.com/FoundationDB/fdb-record-layer/issues/1615)

### 3.1.255.0

* **Feature** Support auto-complete suggestions for phrases [(Issue #1630)](https://github.com/FoundationDB/fdb-record-layer/issues/1630)

### 3.1.254.0

* **Feature** Have separate analyzer pairs for Lucene full-text search and auto-complete suggestions [(Issue #1627)](https://github.com/FoundationDB/fdb-record-layer/issues/1627)

### 3.1.253.0

* **Performance** An optimized BlendedInfixSuggester for auto-complete suggestion, that does not store term vectors [(Issue #1624)](https://github.com/FoundationDB/fdb-record-layer/issues/1624)
* **Performance** A file sequence counter used to map file names to an internal ID by the Lucene directory implementation is now cached in memory [(Issue #1577)](https://github.com/FoundationDB/fdb-record-layer/issues/1577)

### 3.1.252.0


### 3.1.251.0

* **Bug fix** Scans of empty Lucene index now return an empty cursor instead of throwing and error [(Issue #1597)](https://github.com/FoundationDB/fdb-record-layer/issues/1597)
* **Feature** The `LuceneIndexMaintainer` now supports delete where operations if the filter is on the grouping columns of the index [(Issue #1582)](https://github.com/FoundationDB/fdb-record-layer/issues/1582)

### 3.1.250.0

* **Bug fix** FDBDatabaseRunnerImpl correctly trends towards maxDelayMillis, not 0 [(Issue #1565)](https://github.com/FoundationDB/fdb-record-layer/issues/1565)

### 3.1.249.0

* **Bug fix** Delete records where limits indexes on grouping key expressions to predicates that can be satisfied by only the grouping columns [(Issue #1583)](https://github.com/FoundationDB/fdb-record-layer/issues/1583)

### 3.1.248.0

* **Bug fix** Blocking calls within the Lucene index maintainer implementation now use `asyncToSync` to wrap exceptions, control timeouts, and report metrics [(Issue #1571)](https://github.com/FoundationDB/fdb-record-layer/issues/1571)
* **Performance** The Lucene directory implementation now caches the complete list of files in a directory [(Issue #1575)](https://github.com/FoundationDB/fdb-record-layer/issues/1575)
* **Performance** Improve performance of chooseK algorithm by using integer counters for state management instead of Iterator heap objects [(Issue #1590)](https://github.com/FoundationDB/fdb-record-layer/issues/1590)

### 3.1.247.0

* **Bug fix** "rebuilding index failed" does not include primary_key [(Issue #1572)](https://github.com/FoundationDB/fdb-record-layer/issues/1572)
* **Bug fix** Delete records where now handles indexes on key-with-value expressions that split at locations that are in the middle of function key expressions [(Issue #1563)](https://github.com/FoundationDB/fdb-record-layer/issues/1563)

### 3.1.246.0

* **Feature** Lucene plans should support simple comparisons on indexed fields and fielded search [(Issue #1238)](https://github.com/FoundationDB/fdb-record-layer/issues/1238)
* **Breaking change** Adjust Lucene index to use new scan bounds [(Issue #1517)](https://github.com/FoundationDB/fdb-record-layer/issues/1517)
* **Breaking change** Lucene index entry grouping key consistency [(Issue #1528)](https://github.com/FoundationDB/fdb-record-layer/issues/1528)
* **Breaking change** Lucene specific code should be in the lucene module [(Issue #1529)](https://github.com/FoundationDB/fdb-record-layer/issues/1529)

### 3.1.244.0

* **Feature** Type system for Values [(Issue #1545](https://github.com/FoundationDB/fdb-record-layer/issues/1545)

### 3.1.243.0

* **Feature** Index scrubber - return the count of bad entries found [(Issue #1547)](https://github.com/FoundationDB/fdb-record-layer/issues/1547)
* **Feature** Allow scrubbing of non-"value" indexes [(Issue #1551)](https://github.com/FoundationDB/fdb-record-layer/issues/1551)

### 3.1.242.0


### 3.1.241.0


### 3.1.240.0


### 3.1.239.0


### 3.1.238.0

* **Feature** Support custom additional synonyms. This introduces a new SynonymMapRegistry.
New synonym maps should implement `SynonymMapConfig`. See example `EnglishSynonymMap`.

### 3.1.237.0


### 3.1.236.0


### 3.1.235.0

* **Feature** Reconsider IndexMaintainer.scan signature [(Issue #1506)](https://github.com/FoundationDB/fdb-record-layer/issues/1506)

### 3.1.234.0


### 3.1.233.0


### 3.1.232.0

* **Feature** Expose IndexQueryabilityFilter for Aggregate planning [(Issue #1520)](https://github.com/FoundationDB/fdb-record-layer/issues/1520)
* **Breaking change** As part of [(Issue #1520)](https://github.com/FoundationDB/fdb-record-layer/issues/1520) implementers
of `FDBRecordStoreBase` need to implement a new overload of `getSnapshotRecordCountForRecordType` and `evaluateAggregateFunction`
that takes an `IndexQueryabilityFilter`. In addition some methods on `IndexFunctionHelper` and `ComposedBitmapIndexAggregate`
now take an `IndexQueryabilityFilter`; to preserve backwards compatibility, if all indexes are valid,
`IndexQueryabilityFilter.TRUE` can be used.

### 3.1.231.0

* **Feature** Support auto complete suggestions for Lucene search [(Issue #1504)](https://github.com/FoundationDB/fdb-record-layer/issues/1504)

### 3.1.228.0

* **Feature** Chooser and Comparator plans [(Issue #1471)](https://github.com/FoundationDB/fdb-record-layer/issues/1471)
* **Feature** Handle Comparator plan with keys that resolve to repeated fields [(Issue #1501)](https://github.com/FoundationDB/fdb-record-layer/issues/1501)
* **Breaking change** As part of a refactoring of log keys, the log key `LogMessageKeys.ORIGINAL_DATA_SIZE` now displays as `original_data_size` instead of `original_data-size` and a typo has been fixed in the name of `LogMessageKeys.DIRECTORY` (from `DIRECTOY`) [(Issue #1500)](https://github.com/FoundationDB/fdb-record-layer/issues/1500)

### 3.1.227.0

* **Breaking change** Clean up LuceneDocumentFromRecord. Changes definition format for full-text fields; indexes will need to be redefined and rebuilt. [(Issue #1235)](https://github.com/FoundationDB/fdb-record-layer/issues/1235)

### 3.1.226.0


### 3.1.224.0

* **Bug fix** Use 2 separate analyzers for indexing time and query time in LuceneIndexMaintainer [(Issue #1486)](https://github.com/FoundationDB/fdb-record-layer/issues/1486)
* **Feature** Suppor synonym for Lucene indexing [(Issue #1488)](https://github.com/FoundationDB/fdb-record-layer/issues/1488)

### 3.1.223.0

* **Feature** Passing properties configured by adopter into FDBRecordContext [(Issue #1478)](https://github.com/FoundationDB/fdb-record-layer/issues/1478)
* Online Indexer: add time limit to OnlineIndexer.Config [(Issue #1459)](https://github.com/FoundationDB/fdb-record-layer/issues/1459)
* **Breaking change** Support compression for Lucene data. Backwards compatibility is not supported, so upgrading requires rebuilding Lucene indexes during which search requests cannot be completed. [(Issue #1466)](https://github.com/FoundationDB/fdb-record-layer/issues/1466)

### 3.1.222.0

* **Feature** Make QueryPlanResult and QueryPlanInfo immutable [(Issue #1456)](https://github.com/FoundationDB/fdb-record-layer/issues/1456)
* **Breaking change** Up java source and target compatibility to 11 [(Issue #1454)](https://github.com/FoundationDB/fdb-record-layer/issues/1454)
* **Breaking change** Codec with a few optimizations for speeding up compound files sitting on FoundationDB. Backwards compatibility is not supported, so rebuilding Lucene indexes with shutting down search requests handling is needed for transition. [(Issue #1457)](https://github.com/FoundationDB/fdb-record-layer/issues/1457)

## 3.0

### Breaking Changes

This version of the Record Layer removes some legacy elements of the API that were deprecated in previous releases. Most notably, it removes the methods on the `RecordCursor` interface that were compatible with Java `Iterator`s. That API was deprecated in version [2.6](#26) to make it easier for adopters to reason about continuations in asynchronous code by associating each value returned by the cursor with that value's continuation. Adopters still using the deprecated API can either use the `onNext()` and `getNext()` methods on the `RecordCursor` interface or call `asIterator()` to get a `RecordCursorIterator`, which retains compatibility with the `Iterator` interface.

Another, smaller change that has been made is that by default, new indexes added to existing stores (that cannot be built in-line) are now initialized with a `DISABLED` `IndexState` whereas the index used default to a `WRITE_ONLY` state. This means that any records written to the record store prior to the index being built will not perform any I/O to update the index, which is effectively wasted work. However, all indexes must be put in the `WRITE_ONLY` state while they are being built in order to ensure that any updates to the index during the build are captured. This is something that the `OnlineIndexer` should be able to handle automatically for most users, but users of the `ERROR_IF_DISABLED_CONTINUE_IF_WRITE_ONLY` index state precondition may start seeing additional `RecordCoreStorageException`s with the message "Attempted to build non-write-only index" when attempting to build an index. That `IndexStatePrecondition` is not reccommended, however, and users should switch over to using a different `IndexStatePrecondition` (like the default index state precondition, `BUILD_IF_DISABLED_CONTINUE_IF_WRITE_ONLY`) instead or explicitly set the index state on the index to `WRITE_ONLY` prior to building the index. Users can also replicate the old behavior by supplying a `UserVersionChecker` implementation with an appropriate implementation of `needRebuildIndex` to the `FDBRecordStore.Builder`.

### 3.0.221.0

* **Bug fix** allow planner rules to be disabled via planner configuration [(Issue #1445)](https://github.com/FoundationDB/fdb-record-layer/issues/1445)

### 3.0.220.0

* InstrumentedTransaction: Distinguish reads from range_reads and deletes from range_deletes [(Issue #1435)](https://github.com/FoundationDB/fdb-record-layer/issues/1435)

### 3.0.219.0

* **Bug fix** Uniqueness violations encountered in new indexes built during store opening times now fail the index build [(Issue #1371)](https://github.com/FoundationDB/fdb-record-layer/issues/1371)
* **Feature** Streaming aggregate operator implementation [(Issue #1376)](https://github.com/FoundationDB/fdb-record-layer/issues/1376)
* **Feature** Online Indexer: support multi target indexing [(Issue #1398)](https://github.com/FoundationDB/fdb-record-layer/issues/1398)

### 3.0.218.0

* **Feature** Added store timers to track compression efficacy [(Issue #1412)](https://github.com/FoundationDB/fdb-record-layer/issues/1412)

### 3.0.217.0


### 3.0.216.0

* **Feature** Publish per-transaction I/O metrics and commit failure metrics [(Issue #1402)](https://github.com/FoundationDB/fdb-record-layer/issues/1402)

### 3.0.215.0


### 3.0.214.0

* **Bug fix** Fix potential null recordTypeKey in RecordType.getRecordTypeKeyTuple() [(Issue #1395)](https://github.com/FoundationDB/fdb-record-layer/issues/1395)
* **Feature** integrate boolean normalization into planning process [(Issue #1301)](https://github.com/FoundationDB/fdb-record-layer/issues/1301)
* Index Scrubber: missing index entry”: Add the (virtual) missing index’s key to the log message [(Issue #1392)](https://github.com/FoundationDB/fdb-record-layer/issues/1392)

### 3.0.213.0

* **Bug fix** Protect against corrupt input in GeophileQueryTest [(Issue #1377)](https://github.com/FoundationDB/fdb-record-layer/issues/1377)

### 3.0.212.0


### 3.0.211.0


### 3.0.210.0

* **Bug fix** relax conditions enforced in ImplementInUnionRule [(Issue #1369)](https://github.com/FoundationDB/fdb-record-layer/issues/1369)
*OnlineIndexScrubberTest.testScrubberLimits - reduce number of records [(Issue #1363)](https://github.com/FoundationDB/fdb-record-layer/issues/1363)
* OnlineIndexScrubber: clear ranges when exhausted [(Issue #1367)](https://github.com/FoundationDB/fdb-record-layer/issues/1367)
* **Feature** Prototype non-index sorting [(Issue #1161)](https://github.com/FoundationDB/fdb-record-layer/issues/1161)

### 3.0.209.0


### 3.0.208.0


### 3.0.207.0


### 3.0.206.0

* OnlineIndexerIndexFromIndexTest.testIndexFromIndexPersistentContinueRebuildWhenTypeStampChange timeout failures [(Issue #1267)](https://github.com/FoundationDB/fdb-record-layer/issues/1267)

### 3.0.205.0

* **Feature** Cascades planner can plan IN as a dynamic UNION [(Issue #1339)](https://github.com/FoundationDB/fdb-record-layer/issues/1339)
* **Breaking change** InUnion uses a different plan hash [(Issue #1339)](https://github.com/FoundationDB/fdb-record-layer/issues/1339)

### 3.0.204.0


### 3.0.203.0


### 3.0.202.0

* Index scrubber: log warnings when repair is not allowed [(Issue #1345)](https://github.com/FoundationDB/fdb-record-layer/issues/1345)
* Use IndexingPolicy instead of indexStatePrecondition [(Issue #1336)](https://github.com/FoundationDB/fdb-record-layer/issues/1336)

### 3.0.201.0

* **Feature** Indexes can now be marked as being replaced by other indexes [(Issue #1315)](https://github.com/FoundationDB/fdb-record-layer/issues/1315)

### 3.0.200.0


### 3.0.199.0


### 3.0.198.0


### 3.0.197.0

* **Feature** PlanOrderingKey for Intersection / (ordered) Union [(Issue #1319)](https://github.com/FoundationDB/fdb-record-layer/issues/1319)
* **Feature** `AutoContinuingCursor` continues on retriable exceptions [(Issue #1328)](https://github.com/FoundationDB/fdb-record-layer/issues/1328)
* **Feature** Validate and repair LocatableResolver mappings [(Issue #1316)](https://github.com/FoundationDB/fdb-record-layer/issues/1316)
* **Feature** Provide a method to clean readable indexes build data [(Issue #1334)](https://github.com/FoundationDB/fdb-record-layer/issues/1334)

### 3.0.196.0


### 3.0.195.0


### 3.0.194.0


### 3.0.193.0


### 3.0.192.0

* **Bug fix** Fix a typo in DataAccessRule.comparisonKey() [(Issue #1324)](https://github.com/FoundationDB/fdb-record-layer/issues/1324)
* **Feature** Added RecordCursor.asList() with continuation support [(Issue #1307)](https://github.com/FoundationDB/fdb-record-layer/issues/1307)

### 3.0.191.0

* **Bug fix** IN that cannot be made into OR with incompatible sort gets stack overflow [(Issue #1283)](https://github.com/FoundationDB/fdb-record-layer/issues/1283)
* **Feature** IN join with Union instead of concat [(Issue #1286)](https://github.com/FoundationDB/fdb-record-layer/issues/1286)

### 3.0.190.0

* **Bug fix** Index scrubber should consider record filtering [(Issue #1313)](https://github.com/FoundationDB/fdb-record-layer/issues/1313)
* **Feature** Allow stores to check the total size (in bytes) when determining index states rather than the number of records [(Issue #1239)](https://github.com/FoundationDB/fdb-record-layer/issues/1239)

### 3.0.189.0


### 3.0.188.0

* **Feature** Method to convert to ByteString in RecordCursorContinuation [(Issue #1299)](https://github.com/FoundationDB/fdb-record-layer/issues/1299)
* **Feature** FDB server request tracing is now exposed through one of the context configuration options [(Issue #1304)](https://github.com/FoundationDB/fdb-record-layer/issues/1304)

### 3.0.187.0

* **Feature** Add indexing validation & repair [(Issue #1266)](https://github.com/FoundationDB/fdb-record-layer/issues/1266)

### 3.0.186.0

* **Bug fix** Scanning a record store with split records and a scan limit could sometimes result in errors with the message `attempted to return a result with NoNextReason of SOURCE_EXHAUSTED but a non-end continuation` [(Issue #1294)](https://github.com/FoundationDB/fdb-record-layer/issues/1294)

### 3.0.185.0

* **Feature** Multi-valued queryable functions [(Issue #1289)](https://github.com/FoundationDB/fdb-record-layer/issues/1289)

### 3.0.184.0

* **Bug fix** OneOfThemWithComparison.withOtherComparison lost emptyMode [(Issue #1284)](https://github.com/FoundationDB/fdb-record-layer/issues/1284)

### 3.0.183.0

* **Feature** Define query compatibility using store state and pre-bound parameters [(Issue #1275)](https://github.com/FoundationDB/fdb-record-layer/issues/1275)
* **Breaking change** IndexQueryabilityFilter is not a functional interface anymore [(Issue #1275)](https://github.com/FoundationDB/fdb-record-layer/issues/1275)

### 3.0.182.0

* **Bug fix** The `AutoService` annotations are no longer retained at in published jars as was the case prior to [3.0.181.0](#301810) [(Issue #1281)](https://github.com/FoundationDB/fdb-record-layer/issues/1281)

### 3.0.181.0

* **Feature** Methods to estimate the size of a record store are now exposed [(Issue #1229)](https://github.com/FoundationDB/fdb-record-layer/issues/1229)
* **Breaking change** The iterator style `RecordCursor` API is removed [(Issue #1136)](https://github.com/FoundationDB/fdb-record-layer/issues/1136)
* **Breaking change** The `RecordCursor::limitTo` method has been removed in favor of `RecordCursor::limitRowsTo` [(Issue #1189)](https://github.com/FoundationDB/fdb-record-layer/issues/1189)
* **Breaking change** The `RecordCursor::orElse` and `RecordCursor::flatMapPipelined` methods have been removed in favor of variants that allow for continuations to be correctly handled [(Issue #1189)](https://github.com/FoundationDB/fdb-record-layer/issues/1189)
* **Breaking change** Remove non-controversial deprecated methods [(Issue #1191)](https://github.com/FoundationDB/fdb-record-layer/issues/1191)
* **Breaking change** Indexes on existing stores will now default to the `DISABLED` `IndexState` instead of `WRITE_ONLY` if they cannot be built inline [(Issue #1213)](https://github.com/FoundationDB/fdb-record-layer/issues/1213)
* **Breaking change** Increased the versions of our dependencies, including Protobuf (to 3.15.6) and Guava (to 30.1-jre) [(Issue #1193)](https://github.com/FoundationDB/fdb-record-layer/issues/1193)
* **Breaking change** Requires a minimum FoundationDB client and server version of 6.3 [(Issue #1201)](https://github.com/FoundationDB/fdb-record-layer/issues/1201)
## 2.10

### Breaking Changes

In this release, the various implementations of the `RecordQueryPlan` interface have  moved to API stability level `INTERNAL`. This means that individual implementations may change without notice. Clients that are not creating `RecordQueryPlan` objects directly (but instead using the planner to create plans) should not be affected.

### 2.10.180.0

* **Bug fix** LiteralKeyExpression should ignore Value for planHash and queryHash [(Issue #1273)](https://github.com/FoundationDB/fdb-record-layer/issues/1273)

### 2.10.179.0

* **Bug fix** Uses of FDBDatabaseRunner include logging details for exception logging [(Issue #1255)](https://github.com/FoundationDB/fdb-record-layer/issues/1255)
* **Bug fix** Passing a `RecordContext` to a `LocatableResolver` could fail with a non-retriable error when comparing committed and non-committed data in an in-memory cache [(Issue #1258)](https://github.com/FoundationDB/fdb-record-layer/issues/1258)

### 2.10.178.0


### 2.10.177.0

* Add indexing method to build 'index online log' message [(Issue #1242)](https://github.com/FoundationDB/fdb-record-layer/issues/1242)

### 2.10.169.0

* **Feature** Rebuild index when prev by-index continuation isn't possible [(Issue #1232)](https://github.com/FoundationDB/fdb-record-layer/issues/1232)

### 2.10.168.0

* **Feature** Indexing refactor: change OnlineIndexer.IndexFromIndexPolicy to a general OnlineIndexer.IndexingPolicy [(Issue #1223)](https://github.com/FoundationDB/fdb-record-layer/issues/1223)

### 2.10.167.0


### 2.10.166.0

* **Feature** Indexing: Handle the case of requested method mismatches a previous session. Options are: use previous, rebuild, and error [(Issue #1198)](https://github.com/FoundationDB/fdb-record-layer/issues/1198)

### 2.10.165.0

* **Bug fix** allow NULL_UNIQUE as a stand-in for fields that are used to compute covering index optimizations [(Issue #1220)](https://github.com/FoundationDB/fdb-record-layer/issues/1220)
* **Feature** Indexing: log throttle delay [(Issue #1218)](https://github.com/FoundationDB/fdb-record-layer/issues/1218)

### 2.10.164.0


### 2.10.163.0


### 2.10.162.0

* **Bug fix** Planner's key from index should include primary keys [(Issue #1138)](https://github.com/FoundationDB/fdb-record-layer/issues/1138)
* **Performance** RecordQueryPlanner.getKeyForMerge can build redundant merge keys [(Issue #1154)](https://github.com/FoundationDB/fdb-record-layer/issues/1154)
* **Feature** Need a way to tell whether the result of planning a query is only sorted by the requested keys [(Issue #1155)](https://github.com/FoundationDB/fdb-record-layer/issues/1155)

### 2.10.161.0

* **Bug fix** represent an aggregation function as using a set of aggregation keys [(Issue #1175)](https://github.com/FoundationDB/fdb-record-layer/issues/1175)
* **Bug fix** `GroupingValidator` only does one level of nesting [(Issue #1172)](https://github.com/FoundationDB/fdb-record-layer/issues/1172)
* **Feature** Add QueryPlanResult to hold planned query and PlanInfo. [(Issue #1176)](https://github.com/FoundationDB/fdb-record-layer/issues/1176)
* **Feature** Rank by grouped map-like values [(Issue #1183)](https://github.com/FoundationDB/fdb-record-layer/issues/1183)
* **Feature** `VersionstampSaveBehavior.IF_PRESENT` allows the user to specify that a record should be saved with a version only if one is explicitly provided [(Issue #958)](https://github.com/FoundationDB/fdb-record-layer/issues/958)

### 2.10.160.0

* **Bug fix** `RecordQueryPlanner.AndWithThenPlanner.planChild` does not have a case like `planAndWithNesting` [(Issue #1140)](https://github.com/FoundationDB/fdb-record-layer/issues/1140)
* **Bug fix** Covering check when chosen index has duplicate fields [(Issue #1139)](https://github.com/FoundationDB/fdb-record-layer/issues/1139)
* **Bug fix** `LiteralKeyExpression` fails `hasProperInterfaces` [(Issue #1152)](https://github.com/FoundationDB/fdb-record-layer/issues/1152)
* **Bug fix** `@DualPlannerTest` does not distinguish tests [(Issue #1150)](https://github.com/FoundationDB/fdb-record-layer/issues/1150)
* **Performance** represent an aggregation function using a set of aggregation keys rather than a KeyExpression [(Issue #1175)](https://github.com/FoundationDB/fdb-record-layer/issues/1175)

### 2.10.159.0

* **Bug fix** Fix LiteralKeyExpression hash code [(Issue #1066)](https://github.com/FoundationDB/fdb-record-layer/issues/1066)
* **Bug fix** Stable plan from cost perspective [(Issue #1148)](https://github.com/FoundationDB/fdb-record-layer/issues/1148)

### 2.10.158.0

* **Feature** Allow creating a target index by iterating a source index of the same type [(Issue #1078)](https://github.com/FoundationDB/fdb-record-layer/issues/1078)
* **Feature** Add persistent continuation and parallel indexing to indexing by index [(Issue #1107)](https://github.com/FoundationDB/fdb-record-layer/issues/1107)

### 2.10.157.0

* **Feature** Add configuration for Cascades planner, protect from queue explosion and infinite loop, use interface instead of RecordQueryPlanner [(Issue #1126)](https://github.com/FoundationDB/fdb-record-layer/issues/1126)

### 2.10.156.0

* No changes from version [2.10.155.0](#2101550)

### 2.10.155.0

* **Feature** Certain system keys are exposed through the new `FDBSystemOperations` API [(Issue #949)](https://github.com/FoundationDB/fdb-record-layer/issues/949)
* **Performance** Query plan costing model is adjusted to not prefer full record scans over index scans with the same residual filters [(Issue #1130)](https://github.com/FoundationDB/fdb-record-layer/issues/1130)

### 2.10.154.0

* **Bug fix** Scanned records counter was not cleared properly when clearing the index [(Issue #1115)](https://github.com/FoundationDB/fdb-record-layer/issues/1115)
* **Bug fix** Fix the instruction document of building Record Layer [(Issue #1061)](https://github.com/FoundationDB/fdb-record-layer/issues/1061)
* **Feature** Permuted grouped aggregate [(Issue #1085)](https://github.com/FoundationDB/fdb-record-layer/issues/1085)
* **Feature** integrated index matching and planning in the Cascades planner [(Issue #1083)](https://github.com/FoundationDB/fdb-record-layer/issues/1083)
* **Feature** Implement QueryHash to identify created queries and match them to generated plans [(Issue #1091)](https://github.com/FoundationDB/fdb-record-layer/issues/1091)
* **Breaking change** The addition of QueryHashable and the implementation of it by various classes will force existing extenders of some classes to implement the queryHash() method [(Issue #1091)](https://github.com/FoundationDB/fdb-record-layer/issues/1091)

### 2.10.151.0


### 2.10.150.0

* **Feature** Count indexes can be cleared when decremented to zero [(Issue #737)](https://github.com/FoundationDB/fdb-record-layer/issues/737)

### 2.10.149.0

* **Performance** Improvement: OnlineIndexer: Add index name to "build index online" log message [(Issue #1081)](https://github.com/FoundationDB/fdb-record-layer/issues/1081)
* **Feature** Implement different kinds of planHash for use with plan hash that ignores literals [(Issue #1072)](https://github.com/FoundationDB/fdb-record-layer/issues/1072)
* **Breaking change** planHash() is now a default method implementation in PlanHashable, delegating to planHash(PlanHashKind). As a result, code will have to be recompiled to use this default [(Issue #1072)](https://github.com/FoundationDB/fdb-record-layer/issues/1072)

### 2.10.148.0

* **Feature** General index matching framework and mechanics needed for the new planner implementation. [(Issue #1026)](https://github.com/FoundationDB/fdb-record-layer/issues/1026)

### 2.10.147.0

* **Bug fix** "make unbuilt index readable" exception blocks opening a record store [(Issue #1054)](https://github.com/FoundationDB/fdb-record-layer/issues/1054)
* **Feature** Allow degenerate composed bitmaps from single index [(Issue #1068)](https://github.com/FoundationDB/fdb-record-layer/issues/1068)

### 2.10.146.0

* **Feature** Composed bitmaps from map-like fields [(Issue #1057)](https://github.com/FoundationDB/fdb-record-layer/issues/1057)

### 2.10.145.0

* **Bug fix** Record scans with a time limit could result in errors or missing versions [(Issue #1046)](https://github.com/FoundationDB/fdb-record-layer/issues/1046)
* **Feature** Support checking whether or not an index is being built [(Issue #1043)](https://github.com/FoundationDB/fdb-record-layer/issues/1043)

### 2.10.144.0

* **Bug fix** Need grouping / grouped key canonical form [(Issue #1052)](https://github.com/FoundationDB/fdb-record-layer/issues/1052)
* **Performance** preloadRecordAsync() caches negative results [(Issue #1048)](https://github.com/FoundationDB/fdb-record-layer/issues/1048)

### 2.10.142.0

* **Bug fix** IndexFunctionHelper.getGroupedKey / getGroupingKey can get array bounds error [(Issue #1050)](https://github.com/FoundationDB/fdb-record-layer/issues/1050)

### 2.10.141.0

* **Performance** Covering filter optimizations and index-applicable filters are now considered as advantageous during planning [(Issue #1039)](https://github.com/FoundationDB/fdb-record-layer/issues/1039)

### 2.10.140.0

* **Bug fix** Sampled key read operation can log a lot [(Issue #1020)](https://github.com/FoundationDB/fdb-record-layer/issues/1020)
* **Feature** FDB client run-loop profiling can be enabled on the `FDBDatabaseFactory` [(Issue #1041)](https://github.com/FoundationDB/fdb-record-layer/issues/1041)

### 2.10.139.0


### 2.10.138.0

* **Bug fix** Resolves concurrency issue in executing plans with delayed record fetches in multi-threaded environments [(Issue #1024)](https://github.com/FoundationDB/fdb-record-layer/issues/1024)

### 2.10.137.0

* **Bug fix** Requested full text fields always contain the full text, not just a single token [(Issue #1017)](https://github.com/FoundationDB/fdb-record-layer/issues/1017)
* **Performance** Covering filter optimizations can now be performed below union, intersection, and distinct operators [(Issue #1016)](https://github.com/FoundationDB/fdb-record-layer/issues/1016)
* **Feature** Avoid repeating index endpoints conflicts [(Issue #1019)](https://github.com/FoundationDB/fdb-record-layer/issues/1019)

### 2.10.136.0

* **Feature** BITMAP_VALUE index cursors can be bit-wise merged [(Issue #1012)](https://github.com/FoundationDB/fdb-record-layer/issues/1012)
* **Breaking change** Result type of planCoveringAggregateIndex has changed, requiring a recompile of callers [(Issue #1012)](https://github.com/FoundationDB/fdb-record-layer/issues/1012)

### 2.10.135.0

* **Bug fix** OnlineIndexer does not backoff properly when it retries [(Issue #1007)](https://github.com/FoundationDB/fdb-record-layer/issues/1007)
* **Feature** Bitmap value indexes [(Issue #1010)](https://github.com/FoundationDB/fdb-record-layer/issues/1010)
* **Breaking change** Several API changes in `RecordQueryPlan` implementors, which are also now `INTERNAL` APIs. [(Issue #987)](https://github.com/FoundationDB/fdb-record-layer/issues/987)

## 2.9

### Breaking Changes

This version of the Record Layer requires a FoundationDB server version of at least version 6.2. Attempting to connect to older versions may result in the client hanging when attempting to connect to the database.

Additionally, builds for the project now require JDK 11. The project is still targetting JDK 1.8 for both source and binary compatibility, so projects importing the library that have not yet upgraded to the newer JDK should still be able to import the project as before, but developers may need to update their local development environment if they have not already done so.

`FDBRecordStore` does not have `addUniquenessCheck` anymore, which is replaced by `checkUniqueness` in `StandardIndexMaintainer` now; `IndexMaintainer` does not have `updateUniquenessViolations` anymore, which is replaced by `addUniquenessViolation` and `removeUniquenessViolationsAsync` in `StandardIndexMaintainer` now; `StandardIndexMaintainer` now has `updateOneKeyAsync` in place of `updateOneKey` .

`OrElseCursor` now uses a structured continuation instead of the previous pass-through style. This breaks the continuations of all `OrElseCursor`s from previous builds. Furthermore, the fluent `RecordCursor.orElse()` method is deprecated in favor of a static method that also takes a continuation.

### 2.9.134.0

* **Performance** Union, intersection, and distinct operations can now be applied to index entries before fetching the primary record [(Issue #991)](https://github.com/FoundationDB/fdb-record-layer/issues/991)

### 2.9.133.0

* **Performance** Better to scan with OR filter than to Union multiple identical scans with individual filters [(Issue #1004)](https://github.com/FoundationDB/fdb-record-layer/issues/1004)
* **Performance** Some filters can be performed with covering index scan's partial record [(Issue #1000)](https://github.com/FoundationDB/fdb-record-layer/issues/1000)

### 2.9.131.0


### 2.9.130.0

* **Bug fix** Avoid excessively long keys/values logged during assertion checks [(Issue #989)](https://github.com/FoundationDB/fdb-record-layer/issues/989)
* **Feature** BooleanNormalizer could eliminate some redundant disjuncts [(Issue #993)](https://github.com/FoundationDB/fdb-record-layer/issues/993)
* **Feature** OnlineIndexer limits amount of work by transaction size [(Issue #703)](https://github.com/FoundationDB/fdb-record-layer/issues/703)

### 2.9.129.0

* **Performance** Fixes #977: RecordMetaData#recordTypesForIndex is N^2 with Universal index probing [(Issue #977)](https://github.com/FoundationDB/fdb-record-layer/issues/977)

### 2.9.128.0

* **Feature** FDBDatabaseRunner should use FDBRecordContextConfig more [(Issue #983)](https://github.com/FoundationDB/fdb-record-layer/issues/983)
* **Feature** Get rid of finalizer [(Issue #985)](https://github.com/FoundationDB/fdb-record-layer/issues/985)

### 2.9.127.0

* **Bug fix** Event analysis is fooled by incomplete version stamps [(Issue #980)](https://github.com/FoundationDB/fdb-record-layer/issues/980)
* **Performance** Fixes #962: Reuse Ciphers vs. creating them new each time... [(Issue #962)](https://github.com/FoundationDB/fdb-record-layer/issues/962)

### 2.9.126.0

* **Bug fix** False positive of uniqueness violations prevents building indexes [(Issue #901)](https://github.com/FoundationDB/fdb-record-layer/issues/901)
* **Bug fix** Incomplete record versions are no longer cached in a way that can lead to cache entries for one record store polluting the values for another [(Issue #952)](https://github.com/FoundationDB/fdb-record-layer/issues/952)
* **Bug fix** `OrElseCursor` properly support continuations when using the else branch [(Issue #974)](https://github.com/FoundationDB/fdb-record-layer/issues/974)
* **Breaking change** Requires a minimum FoundationDB client and server version of 6.2 [(Issue #702)](https://github.com/FoundationDB/fdb-record-layer/issues/702)
* **Breaking change** Builds now require JDK 11 [(Issue #910)](https://github.com/FoundationDB/fdb-record-layer/issues/910)
* **Breaking change** `FDBRecordStore` does not have `addUniquenessCheck` anymore, which is replaced by `checkUniqueness` in `StandardIndexMaintainer` now; `IndexMaintainer` does not have `updateUniquenessViolations` anymore, which is replaced by `addUniquenessViolation` and `removeUniquenessViolationsAsync` in `StandardIndexMaintainer` now; `StandardIndexMaintainer` now has `updateOneKeyAsync` in replace of `updateOneKey` [(Issue #901)](https://github.com/FoundationDB/fdb-record-layer/issues/901)
* **Breaking change** Methods interacting with the local version cache on an `FDBRecordContext` have been removed as they were previously unsafe and should have been internal only [(Issue #952)](https://github.com/FoundationDB/fdb-record-layer/issues/952)
* **Breaking change** `OrElseCursor` has a new continuation format that is incompatible with the previous continuation format [(Issue #974)](https://github.com/FoundationDB/fdb-record-layer/issues/974)
* **Breaking change** Guava and protobuf dependency versions have been upgraded to 29.0 and 3.12.2 respectively [(Issue #967)](https://github.com/FoundationDB/fdb-record-layer/issues/967)

## 2.8

### Features

The `FDBRecordStore` can now use the global meta-data key that was added to FoundationDB in version 6.1 to cache state that otherwise must be read from the database at store initialization time. When opening any record store, the user can supply an instance of the new `MetaDataVersionStampStoreStateCache` class. (Alternatively, the `FDBDatabase` can be configured with a `MetaDataVersionStampStoreStateCache` that will be shared by all record stores opened using that database.) This class uses the value of the cluster's global meta-data key to detect if cache entries are stale, and it will invalidate older entries whenever the value of that key changes.

This cache invalidation strategy is safe as record stores can now be configured to update the global meta-data version key whenever any of the cached information is changed. To do so, the user must configure their record store to have cacheable meta-data by calling `setStoreStateCacheability(true)`. The record store will then update the global meta-data key whenever the cached state changes. If the record store has not been so configured, then the `MetaDataVersionStampStoreStateCache` will *not* cache the store initialization information in memory, so opening the store will require re-reading this information from the database each time the store is opened.

If the client is only accessing a small number of record stores on each cluster, then using the `MetaDataVersionStampStoreStateCache` can speed up record store initialization time as most record stores should now be able to be opened without reading anything from the database. Additionally, enabling this feature can help with users facing scalability problems on large record stores if loading this information at store creation time becomes a performance bottleneck. However, note that if there are many record stores coexisting on the same cluster, enabling this feature on all record stores may cause the meta-data key to change with high frequency, and it can degrage the effectiveness of the cache. Users are therefore encouraged to consider carefully which record stores require this feature and which ones do not before enabling it.

Additionally, each record store now allows users to set custom fields in the store header. For background, every record store includes one extra message stored in a special location with extra information about the record store that the Record Layer uses to control behavior when manipulating that record store. For example, it includes the version of the `RecordMetaData` that was used the last time the record store was accessed. The header is then read every time a record store is opened, and then it can be used to detect if, for example, the meta-data verion has changed and new index builds need to be scheduled. (There are also ways of caching this information in a way that is transactionally consistent with the Record Store itself to avoid creating a performance bottleneck. See: [Issue #537](https://github.com/FoundationDB/fdb-record-layer/issues/537).) The new feature adds a mechanism for the user to set those fields with data that the user may want to update transactionally with the data but that they do not want to pay the cost of performing an additional read against the database with every operation. For example, sample use cases might include:

* Adding a flag to indicate whether a migration of data from an older record type to a newer type has completed, and then using this flag to control whether the old record type or the new record type is queried.
* Information about the history of the data in the record store, such as how many times the data have been copied between different FoundationDB clusters.
* Frequently read data like custom application-speicific options that might differ between different record stores.

Importantly, it is rather expensive to write this data (as all concurrent operations to the record store will fail with a conflict), and as the data in these fields are read with every transaction, it is not advised that the user write to these fields too often or store too much data in them. However, if there is a relatively small amount of data with a very low write-rate, it might be appropriate to use this feature. This feature also requires that the user set the format version for a record store to [`HEADER_USER_FIELDS_FORMAT_VERSION`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.html#HEADER_USER_FIELDS_FORMAT_VERSION). The default format version was not updated to that version.

A client can now specify a custom `IndexQueryabilityFilter` that determines which indexes should be considered by the query planner.

### Breaking Changes

A new format version, [`CACHEABLE_STATE_FORMAT_VERSION`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.html#CACHEABLE_STATE_FORMAT_VERSION), was introduced in this version of the Record Layer. Users who wish to experience zero downtime when upgrading from earlier versions should initialize all record stores to the previous maximum format version, [`SAVE_VERSION_WITH_RECORD_FORMAT_VERSION`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.html#SAVE_VERSION_WITH_RECORD_FORMAT_VERSION), until all clients have been upgraded to version 2.8. If the update is done without this measure, then older clients running 2.7 or older will not be able to read from any record stores written using version 2.8. The new format version is also required to use the new `MetaDataVersionStampStoreStateCache` class to cache a record store's initialization state.

This version of the Record Layer requires a FoundationDB server version of at least version 6.1. Attempting to connect to older versions may result in the client hanging when attempting to connect to the database.

Constructors of the `RecordQueryUnionPlan` and `RecordQueryIntersectionPlan` have been marked as deprecated in favor of static initializers. This will allow for more flexibility as work on the new planner develops.

### Newly Deprecated 

The non-static `RecordCursor::flatMapPipelined()` method has been deprecated because it is easy to mis-use (by mistaken analogy to the `mapPipelined()` method) and cannot be used with continuations. See [Issue #665](https://github.com/FoundationDB/fdb-record-layer/issues/665) for further explanation.

The `FDBDatabase::getReadVersion()` method has been replaced with the `FDBRecordContext::getReadVersionAsync()` and `FDBRecordContext::getReadVersion()` methods. Though not strictly necessary, users should also replace any uses of `Transaction::getReadVersion()` and `Transaction::setReadVersion()` (on the `Transaction` interface provided by the FoundationDB Java bindings) with `FDBRecordContext::getReadVersionAsync()` and `FDBRecordContext::setReadVersion()` on any transactions created by the Record Layer. This allows the Record Layer to track the version in Java memory which both can then be used to skip a JNI hop if the read version is needed, but it also allows the Record Layer to more accurately track when read versions are retrieved from the database if the user has enabled read version tracking.

### 2.8.125.0

* **Performance** Reduce lock contention on StoreTimer updates [(Issue #953)](https://github.com/FoundationDB/fdb-record-layer/issues/953)
* **Performance** Meta Conflict Range Additions hotspot during large operations [(Issue #960)](https://github.com/FoundationDB/fdb-record-layer/issues/960)

### 2.8.124.0

* **Feature** Errors from `SplitHelper` now have more helpful log info [(Issue #932)](https://github.com/FoundationDB/fdb-record-layer/issues/932)

### 2.8.123.0

* **Bug fix** TupleKeyCountTree.addPrefixChild only ever adds one child [(Issue #947)](https://github.com/FoundationDB/fdb-record-layer/issues/947)

### 2.8.122.0

* All changes from version [2.8.118.30](#2811830)

### 2.8.121.0

* **Feature** Support edge ordering in planner graphs [(Issue #928)](https://github.com/FoundationDB/fdb-record-layer/issues/928)

### 2.8.120.0

* **Bug fix** Latency injection counters published when not in use [(Issue #922)](https://github.com/FoundationDB/fdb-record-layer/issues/922)
* **Feature** Add key/value size enforcement assertions to FDBRecordContext [(Issue #925)](https://github.com/FoundationDB/fdb-record-layer/issues/925)

### 2.8.119.0

* **Bug fix** A race condition was fixed in the `RankIndexMaintainer` that could result in corrupt index data if a record had multiple entries inserted into the index with the same grouping key [(Issue #482)](https://github.com/FoundationDB/fdb-record-layer/issues/482)

### 2.8.118.30

* **Bug fix** Opening a store with `ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES` now allows opening a record store if the data is in the `INDEX_BUILD_SPACE` [(Issue #941)](https://github.com/FoundationDB/fdb-record-layer/issues/941)

### 2.8.118.0

* **Bug fix** `FDBDatabaseRunner`s will now refetch a fresh read version on retry to avoid hitting the same conflict multiple times [(Issue #905)](https://github.com/FoundationDB/fdb-record-layer/issues/905)

### 2.8.117.0

* Releases for the `fdb-record-layer-spatial` library have been re-enabled [(Issue #884)](https://github.com/FoundationDB/fdb-record-layer/issues/884)
* All changes from [2.8.110.29](#2811029)

### 2.8.116.0

* **Performance** The `LocatableResolver::resolve` methods have overloads that allow ancillary transactions started for key space path resolution to avoid starting another read version request [(Issue #864)](https://github.com/FoundationDB/fdb-record-layer/issues/864)
* **Performance** The metrics for text indexes now contain more accurate numbers for reads and writes [(Issue #876)](https://github.com/FoundationDB/fdb-record-layer/issues/876)
* **Performance** Index states are now preloaded in fewer round trips [(Issue #881)](https://github.com/FoundationDB/fdb-record-layer/issues/881)
* **Feature** Replace some aggregate metrics with accurate counters [(Issue #866)](https://github.com/FoundationDB/fdb-record-layer/issues/866)
* Releases for the `fdb-record-layer-spatial` library have been temporarily disabled [(Issue #884)](https://github.com/FoundationDB/fdb-record-layer/issues/884)

### 2.8.110.29

* All changes from [2.8.104.28](#2810428)

### 2.8.110.0

* All changes from [2.8.104.26](#2810426)

### 2.8.109.0

* All changes from version [2.8.104.24](#2810425)

### 2.8.107.0

* **Feature** Enhance KeySpaceCountTree to allow for application-specific resolution [(Issue #852)](https://github.com/FoundationDB/fdb-record-layer/issues/852)

### 2.8.106.0

* **Feature** Rank index adds option for how to handle ties [(Issue #806)](https://github.com/FoundationDB/fdb-record-layer/issues/806)
* **Feature** StoreTimer aggregates can return their componeent events [(Issue #857)](https://github.com/FoundationDB/fdb-record-layer/issues/857)
* **Breaking change** StoreTimer aggregates now require getComponentEvents() method [(Issue #857)](https://github.com/FoundationDB/fdb-record-layer/issues/857)

### 2.8.105.0

* **Feature** The transaction timeout option can now be set on `FDBRecordContext`s [(Issue #848)](https://github.com/FoundationDB/fdb-record-layer/issues/848)
* **Breaking change** Additional methods, `setTransactionTimeoutMillis` and `getTransactionTimeoutMillis`, were added to the `FDBDatabaseRunner` interface that implementors will need to react to [(Issue #848)](https://github.com/FoundationDB/fdb-record-layer/issues/848)
* **Breaking change** In resolving [Issue #848](https://github.com/FoundationDB/fdb-record-layer/issues/848), the semantics of setting a transaction ID were slightly modified so that an explicit `null` ID now also checks the MDC [(PR #849)](https://github.com/FoundationDB/fdb-record-layer/pull/849)

### 2.8.104.28

* **Performance** When an `IN` query is transformed to an `OR`, other query predicates are now normalized to allow for better plans [(Issue #888)](https://github.com/FoundationDB/fdb-record-layer/issues/888)

### 2.8.104.26

* **Feature** Expose random hash as an index option [(Issue #869)](https://github.com/FoundationDB/fdb-record-layer/issues/869)
* **Feature** Rank index adds option for how to handle ties [(Issue #806)](https://github.com/FoundationDB/fdb-record-layer/issues/806)

### 2.8.104.25

* All changes from versions [2.8.102.23](#2810223) and [2.8.102.24](#2810224)

### 2.8.104.0

* **Performance** Aggregate metrics added for the number of reads, writes, and deletes [(Issue #839)](https://github.com/FoundationDB/fdb-record-layer/issues/839)
* **Feature** New exception type `ScanNonReadableIndexException` for scanning non-readable indexes [(Issue #850)](https://github.com/FoundationDB/fdb-record-layer/issues/850)

### 2.8.103.0

* **Feature** Add an index queryability function to `RecordQuery` [(Issue #841)](https://github.com/FoundationDB/fdb-record-layer/issues/841)
* **Feature** Expose bytes/records scanned through `ExecuteState` [(Issue #835)](https://github.com/FoundationDB/fdb-record-layer/issues/835)
* **Breaking change** `ByteScanLimiter` and `RecordScanLimiter` are now interfaces. Instances with various concrete behavior are constructed through factory classes. [(PR #836)](https://github.com/FoundationDB/fdb-record-layer/pull/836)

### 2.8.102.24

* **Performance** If an IN predicate cannot be planned using a nested loop join, we now attempt to plan it as an equivalent OR of equality predicates. [(Issue #860)](https://github.com/FoundationDB/fdb-record-layer/issues/860)
* **Feature** The `RecordQueryPlanner` now has a dedicated object for specifying configuration options. [(Issue #861)](https://github.com/FoundationDB/fdb-record-layer/pull/861)

### 2.8.102.23

* **Performance** Aggregate metrics added for the number of reads, writes, and deletes [(Issue #839)](https://github.com/FoundationDB/fdb-record-layer/issues/839)

### 2.8.102.0

* **Bug fix** NoSuchDirectoryException should include full directory path [(Issue #797)](https://github.com/FoundationDB/fdb-record-layer/issues/797)
* **Bug fix** StoreTimer.getDifference() should not return unchanged metrics [(Issue #832)](https://github.com/FoundationDB/fdb-record-layer/issues/832)
* **Performance** Allow setting hash function used by `RankedSet` [(Issue #828)](https://github.com/FoundationDB/fdb-record-layer/issues/828)

### 2.8.101.0

* **Bug fix** If the `FunctionKeyExpression.Registry` fails to initialize, then the error is now propagated to the user when a function is requested [(Issue #826)](https://github.com/FoundationDB/fdb-record-layer/issues/826)
* **Bug fix** COMMIT_READ_ONLY not part of COMMITS aggregate [(Issue #830)](https://github.com/FoundationDB/fdb-record-layer/issues/830)

### 2.8.100.0


### 2.8.99.0

* **Bug fix** Remove reverse directory cache scan fallback [(Issue #821)](https://github.com/FoundationDB/fdb-record-layer/issues/821)
* **Feature** Fix test to show when function index queries work [(Issue #810)](https://github.com/FoundationDB/fdb-record-layer/issues/810)
* **Feature** Support for aggregate performance metrics [(Issue #801)](https://github.com/FoundationDB/fdb-record-layer/issues/801)
* **Feature** Support tracking index build progress by `IndexBuildState` [(Issue #817)](https://github.com/FoundationDB/fdb-record-layer/issues/817)
* **Feature** System keyspace client log parsing [(Issue #816)](https://github.com/FoundationDB/fdb-record-layer/issues/816)

### 2.8.98.0

* **Bug fix** Revert #794 until #489 is worked out. (Fixes potentially index entry lose if two metadata updates happen in different record stores over the same data in the same transaction.) [(Issue #808)](https://github.com/FoundationDB/fdb-record-layer/issues/808)
* **Performance** Improved loggings for `OnlineIndexer` and `FDBDatabaseRunner` [(Issue #810)](https://github.com/FoundationDB/fdb-record-layer/issues/810)

### 2.8.97.0

* **Feature** Emit `TIME_WINDOW_LEADERBOARD_OVERLAPPING_CHANGED` whenever rebuild needed due to overlapping, not just when rebuilding immediately [(Issue #799)](https://github.com/FoundationDB/fdb-record-layer/issues/799)
* **Feature** Clear existing index data when `rebuildIndexWithNoRecord` [(Issue #794)](https://github.com/FoundationDB/fdb-record-layer/issues/794)

### 2.8.96.0

* **Feature** Support for custom Executor per FDBRecordContext [(Issue #787)](https://github.com/FoundationDB/fdb-record-layer/issues/787)
* **Feature** Better detailed metrics in `RankedSet.add` [(Issue #792)](https://github.com/FoundationDB/fdb-record-layer/issues/792)
* **Feature** Upon a failed record deserialization, the file descriptor is only logged if the logger is at `TRACE` instead of `DEBUG` [(Issue #789)](https://github.com/FoundationDB/fdb-record-layer/issues/789)

### 2.8.95.0

* **Bug fix** OnlineIndexer does not always end the synchronized session after use [(Issue #780)](https://github.com/FoundationDB/fdb-record-layer/issues/780)

### 2.8.94.0

* **Bug fix** Two inequality predicates on a single field index will now be planned as a single range scan on the index [(Issue #765)](https://github.com/FoundationDB/fdb-record-layer/issues/765)
* **Bug fix** Primary keys where one key contained a string or byte array that was a prefix of another could lead to deserialization errors when loading [(Issue #782)](https://github.com/FoundationDB/fdb-record-layer/issues/782)
* **Feature** Support forcefully releasing synchronized session locks and stopping ongoing online index builds [(Issue #748)](https://github.com/FoundationDB/fdb-record-layer/issues/748)

### 2.8.93.0

* **Bug fix** Indexes not marked as write-only before being built online [(Issue #773)](https://github.com/FoundationDB/fdb-record-layer/issues/773)

### 2.8.91.0

* **Bug fix** `TimeWindowLeaderboardIndexMaintainer.negateScoreForHighScoreFirst` handles `Long.MIN_VALUE` [(Issue #776)](https://github.com/FoundationDB/fdb-record-layer/issues/776)
* **Breaking change** A leaderboard index with high-score-first containing the lowest possible score of `Long.MIN_VALUE` will have indexed that entry as the highest possible score and should be forcibly reindexed. [(Issue #776)](https://github.com/FoundationDB/fdb-record-layer/issues/776)

### 2.8.90.0

* **Feature** Support building indexes online in a safer and simpler way [(Issue #727)](https://github.com/FoundationDB/fdb-record-layer/issues/727)
* **Feature** Read and write conflict ranges can now be added on individual records [(Issue #767)](https://github.com/FoundationDB/fdb-record-layer/issues/767)
* **Breaking change** Building indexes with `OnlineIndexer` has now been changed to be safer and simpler. One can set `indexStatePrecondition` to `ERROR_IF_DISABLED_CONTINUE_IF_WRITE_ONLY` and set `useSynchronizedSession` to `false` to make the indexer follow the same behavior as before if necessary. [(Issue #727)](https://github.com/FoundationDB/fdb-record-layer/issues/727)

### 2.8.89.0

* **Feature** SubspaceProviderByKeySpacePath can return the KeySpacePath[(Issue #759)](https://github.com/FoundationDB/fdb-record-layer/issues/759)

### 2.8.88.0

* **Breaking bug fix** Protobuf serialization of `LiteralKeyExpression` is now correct for `Long`s and `byte[]`s. [(Issue #756)](https://github.com/FoundationDB/fdb-record-layer/issues/756)
* **Feature** Fetch the config parameters of online indexer in the beginning of each transaction [(Issue #731)](https://github.com/FoundationDB/fdb-record-layer/issues/731)
* **Feature** `FDBRecordVersion` now has a `getDBVersion` method for extracting the database commit version [(Issue #718)](https://github.com/FoundationDB/fdb-record-layer/issues/718)

### 2.8.87.0

* **Performance** The InExtractor was making duplicates that can easily be avoided [(Issue #735)](https://github.com/FoundationDB/fdb-record-layer/issues/735)

### 2.8.86.0


### 2.8.85.0

* **Bug fix** `Field.oneOfThem(OneOfThemEmptyMode mode)` for when the repeated field can otherwise distinguish null or null behavior is not desired [(Issue #725)](https://github.com/FoundationDB/fdb-record-layer/issues/725)
* **Feature** `FDBRecordContext` now exposes the `MAX_TR_ID_SIZE` constant containing the maximum admissable length of a transaction ID [(Issue #721)](https://github.com/FoundationDB/fdb-record-layer/issues/721)

### 2.8.84.0

* **Bug fix** Transaction IDs that would exceed the maximum FDB limit are now truncated or ignored instead of throwing an error [(Issue #705)](https://github.com/FoundationDB/fdb-record-layer/issues/705)
* **Feature** Support validating missing index entries in index entry validation [(Issue #712)](https://github.com/FoundationDB/fdb-record-layer/issues/712)
* **Feature** The store header now supports allowing the user to set custom fields with application-specific information [(Issue #704)](https://github.com/FoundationDB/fdb-record-layer/issues/704)

### 2.8.83.0

* **Performance** The `OnlineIndexer` now instruments its read versions so that performance issues that may be linked to high read version latencies can now be better diagnosed [(Issue #696)](https://github.com/FoundationDB/fdb-record-layer/issues/696)
* **Feature** The transaction priority can be set through the `FDBTransactionPriority` enum, and the `OnlineIndexer` exposes that as an option on index builds [(Issue #697)](https://github.com/FoundationDB/fdb-record-layer/issues/697)
* **Breaking change** The `FDBDatabase::getReadVersion` method has been deprecated and replaced with `FDBRecordContext::getReadVersionAsync` [(Issue #698)](https://github.com/FoundationDB/fdb-record-layer/issues/698)

### 2.8.82.0

* **Feature** Support synchronized sessions, where only one of them is allowed to work at a time [(Issue #637)](https://github.com/FoundationDB/fdb-record-layer/issues/637)
* **Feature** The new `MAX_EVER_VERSION` index type tracks the maximum ever value for key expressions including a record's version [(Issue #670)](https://github.com/FoundationDB/fdb-record-layer/issues/670)
* **Feature** A new store existence check level only warns if there is no store info header but the store contains no records [(Issue #707)](https://github.com/FoundationDB/fdb-record-layer/issues/707)
* **Breaking change** `FDBDatabaseRunner` is changed to an interface, which can be contructed by `FDBDatabaseRunnerImpl` [(Issue #637)](https://github.com/FoundationDB/fdb-record-layer/issues/637)

### 2.8.81.0

* **Feature** Add the capability to take a snapshot of a timer and to subtract said snapshot from the timer. [(Issue #692)](https://github.com/FoundationDB/fdb-record-layer/issues/692)

### 2.8.80.0

* **Feature** Store state information can now be cached using the meta-data key added in FoundationDB 6.1 [(Issue #537)](https://github.com/FoundationDB/fdb-record-layer/issues/537)
* **Feature** Trace log format can be set in the `FDBDatabaseFactory` [(Issue #584)](https://github.com/FoundationDB/fdb-record-layer/issues/584)
* **Breaking change** The version of the `commons-lang3` dependency has been upgraded to version 3.9 [(Issue #606)](https://github.com/FoundationDB/fdb-record-layer/issues/606)
* **Breaking change** The stabilities of `FDBDatabaseRunner` constructors have been downgraded to `INTERNAL` [(Issue #681)](https://github.com/FoundationDB/fdb-record-layer/issues/681)
* **Breaking change** The slf4j dependency has been added to `fdb-extensions` [(Issue #680)](https://github.com/FoundationDB/fdb-record-layer/issues/680)
* **Breaking change** The Record Layer now requires a minimum FoundationDB version of 6.1 [(Issue #551)](https://github.com/FoundationDB/fdb-record-layer/issues/551)

## 2.7

### Breaking Changes

The Guava version has been updated to version 27. Users of the [shaded variants](Shaded.html#Variants) of the Record Layer dependencies should not be affected by this change. Users of the unshaded variant using an older version of Guava as part of their own project may find that their own Guava dependency is updated automatically by their dependency manager.

### 2.7.79.0

* **Bug fix** Fields no longer compare their `NullStandIn` value in their equality check [(Issue #682)](https://github.com/FoundationDB/fdb-record-layer/issues/682)

### 2.7.78.0

* **Bug fix** Fix sorting with null-is-unique indexes [(Issue #655)](https://github.com/FoundationDB/fdb-record-layer/issues/655)
* **Bug fix** `NullStandin` should be set in `FieldKeyExpression::toProto` [(Issue #626)](https://github.com/FoundationDB/fdb-record-layer/issues/626)
* **Deprecated** The non-static `RecordCursor::flatMapPipelined()` method has been deprecated [(Issue #665)](https://github.com/FoundationDB/fdb-record-layer/issues/665)
* All changes from versions [2.7.73.21](#277321) and [2.7.73.22](#277322)

### 2.7.77.0

* **Bug fix** Work around SpotBugs bug [(Issue #659)](https://github.com/FoundationDB/fdb-record-layer/issues/659)
* **Feature** Add experimental join index support [(Issue #159)](https://github.com/FoundationDB/fdb-record-layer/issues/159)

### 2.7.76.0

* **Bug fix** Revert fix for #646 (in [2.7.75.0](#27750)) until a complete resolution is worked out [(PR #654)](https://github.com/FoundationDB/fdb-record-layer/pull/654)
* All changes from version [2.7.73.20](#277320)

### 2.7.75.0

* **Bug fix** Fix key comparison in `checkPossiblyRebuildRecordCounts` [(Issue #646)](https://github.com/FoundationDB/fdb-record-layer/issues/646)

### 2.7.74.0

* **Bug fix** Wrap the `RecordStoreState` in an `AtomicReference` [(Issue #563)](https://github.com/FoundationDB/fdb-record-layer/issues/563)
* **Performance** Blocking on a future that should already be complete can now be detected and logged [(Issue #641)](https://github.com/FoundationDB/fdb-record-layer/issues/641)
* **Performance** The `performNoOp` method on `FDBDatabase` allows for instrumenting the delay from queuing work onto the network thread [(Issue #650)](https://github.com/FoundationDB/fdb-record-layer/issues/650)

### 2.7.73.22

* **Feature** Include timeout counters in `StoreTimer::getKeysAndValues` [(Issue #672)](https://github.com/FoundationDB/fdb-record-layer/issues/672)
* **Feature** Add `TRACE`-level log details for inspecting the `KeyValueUnsplitter` [(Issue #673)](https://github.com/FoundationDB/fdb-record-layer/issues/673)

### 2.7.73.21

* **Bug fix** The `SizeStatisticsCursorContinuation` must not access the continuation of its underlying cursor after it is exhausted [(Issue #656)](https://github.com/FoundationDB/fdb-record-layer/issues/656)

### 2.7.73.20

* **Bug fix** Revert #627 until a complete version of #647 is worked out. [(Issue #646)](https://github.com/FoundationDB/fdb-record-layer/issues/646)

### 2.7.73.0

* **Bug fix** Union fields for record types are now assigned based on field number rather than declaration order [(Issue #620)](https://github.com/FoundationDB/fdb-record-layer/issues/620)
* **Feature** The `MetaDataEvolutionValidator` now permits record types to be renamed as long as all occurences of the record type in the meta-data definition are also renamed [(Issue #508)](https://github.com/FoundationDB/fdb-record-layer/issues/508)

### 2.7.72.0

* **Bug fix** `NullStandin` should be set in `FieldKeyExpression::toProto` [(Issue #626)](https://github.com/FoundationDB/fdb-record-layer/issues/626)
* **Feature** Added support for named, asynchronous post-commit hooks [(Issue #622)](https://github.com/FoundationDB/fdb-record-layer/issues/622)

### 2.7.71.0

* **Bug fix** `OnlineIndexer.buildEndpoints` should not decrease the scan limits on conflicts [(Issue #511)](https://github.com/FoundationDB/fdb-record-layer/issues/511)
* **Bug fix** `KeyValueCursor.Builder.build` now requires `subspace`, `context` and `scanProperties` are specifed, throws RecordCoreException if not [(Issue #418)](https://github.com/FoundationDB/fdb-record-layer/issues/418)
* **Feature** Added a counter counting indexes that need to be rebuilt and changed related logging to `DEBUG` [(Issue #604)](https://github.com/FoundationDB/fdb-record-layer/issues/604)

### 2.7.70.0

* **Breaking change** The deprecated `TimeLimitedCursor` and `RecordCursor.limitTimeTo()` methods have been removed [(Issue #582)](https://github.com/FoundationDB/fdb-record-layer/issues/582)
* **Breaking change** The Guava dependency has been upgraded to version 27.0.1 [(Issue #216)](https://github.com/FoundationDB/fdb-record-layer/issues/216)
* **Deprecated** The `OnlineIndexer::asyncToSync` method has been deprecated [(Issue #473)](https://github.com/FoundationDB/fdb-record-layer/issues/473)

## 2.6

### Breaking Changes

The formerly experimental API for advancing a `RecordCursor` while ensuring correct continuation handling is now preferred. Specifically, the `RecordCursor.onNext()` method is now stable. Meanwhile, the `AsyncIterator`-style methods (such as `onHasNext()`, `hasNext()`, and `next()`) are deprecated. Clients should transition away from these methods before the 2.7 release.

To ease the transition, a new `AsyncIterator` called `RecordCursorIterator` provides these method and can built form a `RecordCursor` using `RecordCursor.asIterator()`.

The `IndexEntry` class now contains a reference to its source index. This meant breaking the constructor on that class. This class should probably only have been instantiated from within the Record Layer, so its old constructors have been removed, and the new constructors are now marked as [`INTERNAL`](https://javadoc.io/page/org.foundationdb/fdb-extensions/latest/com/apple/foundationdb/annotation/API.Status.html#INTERNAL).

The API stability annotations have been moved to their own package. This allows for references to the stability levels within Javadoc pages to link to the annotation documentation. Any external users who had also used these annotations will be required to modify their imports to match the new package. Because these annotations have `CLASS` retention level, this change also requires that any client who depends on the Record Layer update any transitive dependency on `fdb-extensions` to 2.6 to avoid compile-time warnings about missing annotation classes.

The `IndexMaintainer` class now has a new public method `validateEntries`. Subclasses inheriting it should implement the new method. It does not break classes extending `StandardIndexMaintainer` which has a default implementation that does nothing.

The `SubspaceProvider` interface is changed to no longer require an `FDBRecordContext` at construction. Instead, methods that resolve subspaces are directly provided with an `FDBRecordContext`. This provides implementations with the flexibility to resolve logical subspace representations against different FoundationDB backends.

This version of the Record Layer requires a FoundationDB server version of at least version 6.0. Attempting to connect to older versions may result in the client hanging when attempting to connect to the database.

The `getTimeToLoad` and `getTimeToDeserialize` methods on `FDBStoredRecord` have been removed. These were needed for a short-term experiment but stuck around longer than intended.

### Newly Deprecated

Methods for retrieving a record from a record store based on an index entry generally took both an index and an index entry. As index entries now store a reference to their associatedindex, these methods have been deprecated in favor of methods that only take an index entry. The earlier methods may be removed in a future release. The same is true for a constructor on `FDBIndexedRecord` which no longer needs to take both an index and an index entry.

While not deprecated, the [`MetaDataCache`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/MetaDataCache.html) interface's stability has been lessened from [`MAINTAINED`](https://javadoc.io/page/org.foundationdb/fdb-extensions/latest/com/apple/foundationdb/annotation/API.Status.html#MAINTAINED) to [`EXPERIMENTAL`](https://javadoc.io/page/org.foundationdb/fdb-extensions/latest/com/apple/foundationdb/annotation/API.Status.html#EXPERIMENTAL). That interface may undergo further changes as work progresses on [Issue #280](https://github.com/FoundationDB/fdb-record-layer/issues/280). Clients are discouraged from using that interface until work evolving that API has progressed.

The static `loadRecordStoreStateAsync` methods on `FDBRecordStore` were deprecated in 2.6.61.0 and removed in 2.6.66.0. Users are encouraged to use `getRecordStoreState` instead.

The `RecordStoreState` constructors have been deprecated or marked [`INTERNAL`](https://javadoc.io/page/org.foundationdb/fdb-extensions/latest/com/apple/foundationdb/annotation/API.Status.html#INTERNAL) as part of the work adding store state caching ([Issue #410](https://github.com/FoundationDB/fdb-record-layer/issues/410)). They are likely to be further updated as more meta-data is added to the information stored on indexes within each record store ([Issue #506](https://github.com/FoundationDB/fdb-record-layer/issues/506)).

### 2.6.69.0

* **Bug fix** Deleting all records no longer resets all index states to `READABLE` [(Issue #399)](https://github.com/FoundationDB/fdb-record-layer/issues/399)
* **Bug fix** Rebuilding all indexes with `FDBRecordStore::rebuildAllIndexes` now updates the local cache of index states [(Issue #597)](https://github.com/FoundationDB/fdb-record-layer/issues/597)
* **Feature** Include index information and UUID in OnlineIndexer logs [(Issue #510)](https://github.com/FoundationDB/fdb-record-layer/issues/510)
* **Feature** Ability to repair records missing the unsplit record suffix [(Issue #588)](https://github.com/FoundationDB/fdb-record-layer/issues/588)
* **Feature** Provide a way for `FDBDatabase` to use a non-standard locality API  [(Issue #504)](https://github.com/FoundationDB/fdb-record-layer/issues/504)

### 2.6.68.0

* **Bug fix** OnlineIndexer re-increasing limit log changed to info [(Issue #570)](https://github.com/FoundationDB/fdb-record-layer/issues/570)
* **Bug fix** Updating the store header now updates the `last_update_time` field with the current time [(Issue #593)](https://github.com/FoundationDB/fdb-record-layer/issues/593)
* **Feature** Include index information and UUID in OnlineIndexer logs [(Issue #510)](https://github.com/FoundationDB/fdb-record-layer/issues/510)

### 2.6.67.0

* **Bug fix** `FilterCursor`s now schedule all asychronous work using the cursor's executor [(Issue #573)](https://github.com/FoundationDB/fdb-record-layer/issues/573)
* **Bug fix** `FDBRecordStore.checkRebuildIndexes` now uses the count of all records to determine overall emptiness [(Issue #577)](https://github.com/FoundationDB/fdb-record-layer/issues/577)

### 2.6.66.0

* **Bug fix** A new store with `metaDataVersion` of `0` would not set that field, confusing a later load [(Issue #565)](https://github.com/FoundationDB/fdb-record-layer/issues/565)
* **Breaking change** Remove the `loadRecordStoreStateAsync` static methods [(Issue #559)](https://github.com/FoundationDB/fdb-record-layer/issues/559)

### 2.6.64.0

* **Feature** The `FDBRecordStoreStateCache` allows for users to initialize a record store from a cached state. [(Issue #410)](https://github.com/FoundationDB/fdb-record-layer/issues/410)

### 2.6.63.0

* **Breaking change** The experimental `TimeWindowLeaderboardScoreTrim` operation now takes `Tuple` instead of `IndexEntry` [(Issue #554)](https://github.com/FoundationDB/fdb-record-layer/issues/554)

### 2.6.62.0

* **Feature** Add `FDBRecordStore.getAllIndexStates` [(Issue #552)](https://github.com/FoundationDB/fdb-record-layer/issues/552)

### 2.6.61.0

* **Bug fix** `preloadRecordAsync` gets NPE if key doesn't exist [(Issue #541)](https://github.com/FoundationDB/fdb-record-layer/issues/541)
* **Performance** `OnlineIndexer` inherits `WeakReadSemantics` [(Issue #519)](https://github.com/FoundationDB/fdb-record-layer/issues/519)
* **Performance** Values are cached by the `AsyncLoadingCache` instead of futures to avoid futures sharing errors or timeouts [(Issue #538)](https://github.com/FoundationDB/fdb-record-layer/issues/538)
* **Performance** `ProbableIntersectionCursor`s and `UnorderedUnionCursor`s now throw an error if they take longer than 15 seconds to find a next state as a temporary debugging measure [(Issue #546)](https://github.com/FoundationDB/fdb-record-layer/issues/546)
* **Feature** The bytes and record scanned query limits can now be retrieved through the `ExecuteProperties` object [(Issue #544)](https://github.com/FoundationDB/fdb-record-layer/issues/544)
* **Breaking change** Deprecate the static `loadRecordStoreStateAsync` methods from `FDBRecordStore` [(Issue #534)](https://github.com/FoundationDB/fdb-record-layer/issues/534)

### 2.6.60.0

* All changes from version [2.5.54.18](#255418)

### 2.6.59.0

* **Performance** Traced transactions restore MDC context [(Issue #529)](https://github.com/FoundationDB/fdb-record-layer/issues/529)
* **Feature** Automatically add a default union to the record meta-data if missing [(Issue #204)](https://github.com/FoundationDB/fdb-record-layer/issues/204)

### 2.6.58.0

* **Feature** Ability to inject latency into transactions [(Issue #521)](https://github.com/FoundationDB/fdb-record-layer/issues/521)

### 2.6.57.0

* **Feature** The new `ConcatCursor` allows clients to chain the results of two cursors together [(Issue #13)](https://github.com/FoundationDB/fdb-record-layer/issues/13)

### 2.6.56.0

* All changes from version [2.5.54.17](#255417)

### 2.6.55.0

* **Bug fix** getVersionstamp() future can complete a tiny bit after the commit() future [(Issue #476)](https://github.com/FoundationDB/fdb-record-layer/issues/476)
* **Feature** A new cursor type `AutoContinuingCursor` which can iterate over a cursor across transactions [(Issue #397)](https://github.com/FoundationDB/fdb-record-layer/issues/397)
* **Feature** A new `AsyncIterator` that wraps `RecordCursor` provides an easy migration path away from the old methods on `RecordCursor` [(Issue #368)](https://github.com/FoundationDB/fdb-record-layer/issues/368)
* **Feature** The `getNext` method on `RecordCursor`s now provides a blocking version of `onNext` [(Issue #408)](https://github.com/FoundationDB/fdb-record-layer/issues/408)
* **Breaking change** `IndexEntry` objects now contain a reference to their associated index [(Issue #403)](https://github.com/FoundationDB/fdb-record-layer/issues/403)
* **Breaking change** Remove deprecated `KeySpacePath` APIs [(Issue #169)](https://github.com/FoundationDB/fdb-record-layer/issues/169)
* **Breaking change** `IndexMaintainer`s should implement `validateEntries` to validate orphan index entries [(Issue #383)](https://github.com/FoundationDB/fdb-record-layer/issues/383)
* **Breaking change** The API stability annotations have been moved into `com.apple.foundationdb.annotation` [(Issue #406)](https://github.com/FoundationDB/fdb-record-layer/issues/406)
* **Breaking change** `SubspaceProvider` receives an `FDBRecordContext` when a subspace is resolved instead of when constructed. [(Issue #338)](https://github.com/FoundationDB/fdb-record-layer/issues/338)
* **Breaking change** The `MetaDataCache` interface is now `EXPERIMENTAL` [(Issue #447)](https://github.com/FoundationDB/fdb-record-layer/issues/447)
* **Breaking change** The `AsyncIterator` methods on `RecordCursor` are now deprecated [(Issue #368)](https://github.com/FoundationDB/fdb-record-layer/issues/368)
* **Breaking change** The Record Layer now requires a minimum FoundationDB version of 6.0 [(Issue #313)](https://github.com/FoundationDB/fdb-record-layer/issues/313)
* **Breaking change** Remove per-record time-to-load [(Issue #461)](https://github.com/FoundationDB/fdb-record-layer/issues/461)

## 2.5

### Breaking Changes

In order to simplify typed record stores, the `FDBRecordStoreBase` class was turned into an interface and all I/O was placed in the `FDBRecordStore` class. This makes the `FDBTypedRecordStore` serve as only a type-safe wrapper around the `FDBRecordStore` class. As part of this work, the `IndexMaintainer` interface and its implementations lost their type parameters and now only interact with `Message` types, and the `OnlineIndexerBase` class was removed. Additionally, the `FDBEvaluationContext` class was removed in favor of using `EvaluationContext` (without a type parameter) directly. That class also no longer carries around an `FDBRecordStore` reference, so query plans now require an explicit record store in addition to an evaluation context. Finally, the `evaluate` family of methods on key expressions no longer take evaluation contexts. Users should switch any uses of the `OnlineIndexerBase` to a generic `OnlineIndexer` and will need to update any explicit key expression evluation calls. Users should also switch from calling `RecordQueryPlan::execute` to calling `FDBRecordStore::executeQuery` if possible as that second API is more stable (and was not changed as part of the recent work).

### Newly Deprecated

The `asyncToSync` method of the `OnlineIndexer` has been marked as `INTERNAL`. Users should transition to using one of the `asyncToSync` methods defined on either `FDBDatabase`, `FDBRecordContext`, or `FDBDatabaseRunner`. This method may be removed from our public API in a later release (see [Issue # 473](https://github.com/FoundationDB/fdb-record-layer/issues/473)).

### 2.5.54.18

* **Bug fix** `ProbableIntersectionCursor`s and `UnorderedUnionCursor`s should no longer get stuck in a loop when one child completes exceptionally and another with a limit [(Issue #526)](https://github.com/FoundationDB/fdb-record-layer/issues/526)

### 2.5.54.17

* **Bug fix** Added an optional limit on the number of suppressed exceptions [(Issue #512)](https://github.com/FoundationDB/fdb-record-layer/issues/512)

### 2.5.54.0

* **Bug fix** The preload cache in `FDBRecordStore` now invalidates entries corresponding to updated records. [(Issue #494)](https://github.com/FoundationDB/fdb-record-layer/issues/494)
* **Feature** Include index subspace key in "Attempted to make unbuilt index readable" message [(Issue #470)](https://github.com/FoundationDB/fdb-record-layer/issues/470)

### 2.5.53.0

* **Feature** `FDBMetaDataStore` class now has convenience methods for adding and deprecating record types and fields [(Issue #376)](https://github.com/FoundationDB/fdb-record-layer/issues/376)
* All changes from version [2.5.49.16](#254916)

### 2.5.52.0

* **Bug fix** KeyValueLogMessage now converts `"` to `'` in values [(Issue #472)](https://github.com/FoundationDB/fdb-record-layer/issues/472)
* **Feature** Add `RecordMetaDataBuilder.addFormerIndex` [(Issue #485)](https://github.com/FoundationDB/fdb-record-layer/issues/485)
* **Deprecated** The `asyncToSync` method of the `OnlineIndexer` has been marked `INTERNAL` [(Issue #474)](https://github.com/FoundationDB/fdb-record-layer/issues/474)

### 2.5.51.0

* **Bug fix** `OnlineIndexer` fails to `getPrimaryKeyBoundaries` when there is no boundary [(Issue #460)](https://github.com/FoundationDB/fdb-record-layer/issues/460)
* **Bug fix** The `markReadableIfBuilt` method of `OnlineIndexer` now waits for the index to be marked readable before returning [(Issue #468)](https://github.com/FoundationDB/fdb-record-layer/issues/468)

### 2.5.50.0

* **Feature** Add methods in `OnlineIndexer` to support building an index in parallel [(Issue #453)](https://github.com/FoundationDB/fdb-record-layer/issues/453)

### 2.5.49.16

* **Feature** OnlineIndexer logs number of records scanned [(Issue #479)](https://github.com/FoundationDB/fdb-record-layer/issues/479)
* **Feature** OnlineIndexer includes range being built in retry logs [(Issue #480)](https://github.com/FoundationDB/fdb-record-layer/issues/480)

### 2.5.49.0

* **Feature** Add an `AsyncIteratorCursor` between `IteratorCursor` and `KeyValueCursor` [(Issue #449)](https://github.com/FoundationDB/fdb-record-layer/issues/449)
* **Feature** OnlineIndexer can increase limit after successful range builds [(Issue #444)](https://github.com/FoundationDB/fdb-record-layer/issues/444)

### 2.5.48.0

* **Bug fix** Use ContextRestoringExecutor for FDBDatabaseRunner [(Issue #436)](https://github.com/FoundationDB/fdb-record-layer/issues/436)
* **Feature** Add a method to get primary key boundaries in `FDBRecordStore` [(Issue #439)](https://github.com/FoundationDB/fdb-record-layer/issues/439)
* All changes from version [2.5.44.14](#254414)
* All changes from version [2.5.44.15](#254415)

### 2.5.47.0

* **Feature** `RankedSet` and the `RANK` and `TIME_WINDOW_LEADERBOARD` index types that use it now support operations for getting the rank of a score regardless of whether the set contains that value [(Issue #425)](https://github.com/FoundationDB/fdb-record-layer/issues/425), [(Issue #426)](https://github.com/FoundationDB/fdb-record-layer/issues/426)

### 2.5.46.0

* **Bug fix** Record type and index subspace keys with identical serializations are now validated for collisions [(Issue #394)](https://github.com/FoundationDB/fdb-record-layer/issues/394)
* **Bug fix** Byte scan limit now reliably throws exceptions when `ExecuteProperties.failOnScanLimitReached` is `true` [(Issue #422)](https://github.com/FoundationDB/fdb-record-layer/issues/422)
* **Feature** By default, `FDBMetaDataStore`s are now initialized with an extension registry with all extensions from `record_metadata_options.proto` [(Issue #352)](https://github.com/FoundationDB/fdb-record-layer/issues/352)
* **Feature** `FDBMetaDataStore` class now has convenience methods for `addIndex`, `dropIndex` and `updateRecords` [(Issue #281)](https://github.com/FoundationDB/fdb-record-layer/issues/281)
* **Feature** Index subspace keys can now be assigned based on a counter [(Issue #11)](https://github.com/FoundationDB/fdb-record-layer/issues/11)

### 2.5.45.0

* **Bug fix** The `AsyncLoadingCache` could cache exceptional futures in rare scenarios [(Issue #395)](https://github.com/FoundationDB/fdb-record-layer/issues/395)
* **Feature** A `MetaDataEvolutionValidator` now can be used to ensure that meta-data changes are compatible with the [schema evolution guidelines](SchemaEvolution.md) [(Issue #85)](https://github.com/FoundationDB/fdb-record-layer/issues/85)

### 2.5.44.15

* **Bug fix** The `UnorderedUnionCursor` should now propagate errors from its children instead of sometimes swallowing the exception [(Issue #437)](https://github.com/FoundationDB/fdb-record-layer/issues/437)

### 2.5.44.14

* **Feature** Optionally log successful progress in OnlineIndexer [(Issue #429)](https://github.com/FoundationDB/fdb-record-layer/issues/429)

### 2.5.44.0

* **Feature** The `BooleanNormalizer` now gives up early if given an expression that is too complex [(Issue #356)](https://github.com/FoundationDB/fdb-record-layer/issues/356)

### 2.5.43.0

* **Bug fix** ChainedCursor does not obey byte/record/time scan limits [(Issue #358)](https://github.com/FoundationDB/fdb-record-layer/issues/358)
* **Bug fix** `FDBRecordStore.IndexUniquenessCheck.check` needs to be async [(Issue #357)](https://github.com/FoundationDB/fdb-record-layer/issues/357)
* **Bug fix** The `TimeLimitedCursor` could throw an error when setting its result's `NoNextReason` if the inner cursor was exhausted exactly as the time limit was reached [(Issue #380)](https://github.com/FoundationDB/fdb-record-layer/issues/380)
* **Feature** Collating indexes [(Issue #249)](https://github.com/FoundationDB/fdb-record-layer/issues/249)

### 2.5.42.0

* **Feature** Build record meta-data using user's local / evolved meta-data [(Issue #3)](https://github.com/FoundationDB/fdb-record-layer/issues/3)
* **Feature** Validation checks now reject a `RecordMetaData` object that defines no record types [(Issue #354)](https://github.com/FoundationDB/fdb-record-layer/issues/354)
* **Feature** A new cursor type allows for intersecting cursors with incompatible orderings [(Issue #336)](https://github.com/FoundationDB/fdb-record-layer/issues/336)
* **Feature** The text search query API now exposes `containsAllPrefixes` and `containsAnyPrefix` predicates [(Issue #343)](https://github.com/FoundationDB/fdb-record-layer/issues/343)

### 2.5.41.0

* **Bug fix** Directory layer cache size was accidentally defaulted to zero [(Issue #251)](https://github.com/FoundationDB/fdb-record-layer/issues/251)
* **Performance** Remove blocking calls from LocatableResolver constructors [(Issue #261)](https://github.com/FoundationDB/fdb-record-layer/issues/261)
* **Feature** Tracing diagnostics to detect blocking calls in an asynchronous context [(Issue #262)](https://github.com/FoundationDB/fdb-record-layer/issues/262)
* **Feature** New limit on the number of bytes scanned while executing a cursor [(Issue #349)](https://github.com/FoundationDB/fdb-record-layer/issues/349)

### 2.5.40.0

* All changes from version [2.3.32.13](#233213)

### 2.5.38.0

* **Feature** A KPI to track occurrences of index entries that do not point to valid records [(Issue #310)](https://github.com/FoundationDB/fdb-record-layer/issues/310)
* **Feature** The shaded artifacts now include the `fdb-extensions` library and no longer include guava as a transitive dependency [(Issue #329)](https://github.com/FoundationDB/fdb-record-layer/issues/329)

### 2.5.37.0

* **Feature** The `TextCursor` class now reports metrics on keys read [(Issue #242)](https://github.com/FoundationDB/fdb-record-layer/issues/242)
* **Breaking change** Typed record stores are now wrappers around generic record stores, and several classes no longer take a type parameter [(Issue #165)](https://github.com/FoundationDB/fdb-record-layer/issues/165)
* All changes from version [2.4.35.12](#243512)

## 2.4

### Features

The `RecordCursor` API has been reworked to make using continuations safer and less error prone as well as generally making safer use easier. Previously, a continuation could be retrieved from a `RecordCursor` by calling the `getContinuation` method. However, this method was only legal to call at certain times which made it painful and error-prone to use in asynchronous contexts. (Similar problems existed with the `getNoNextReason` method.) The new API is designed to make correct use more natural. Users are now encouraged to access elements through the new `onNext` method. This method returns the next element of the cursor if one exists or a reason why the cursor cannot return an element if it cannot. It also will always return a valid continuation that can be used to resume the cursor. As the `RecordCursor` interface is fairly fundamental to using the Record Layer, the old API has not yet been deprecated.

### Breaking Changes

The `GlobalDirectoryResolverFactory` class was removed. This is part of a larger effort to migrate away from the `ScopedDirectoryLayer` class in favor of the `ExtendedDirectoryLayer` class when resolving `DirectoryLayerDirectories` as part of a record store's `KeySpacePath`. Users can still access the global directory layer by calling `ExtendedDirectoryLayer.global()`.

The version of the Protocol Buffers dependency was updated to 2.5.0 and 3.6.1 for artifacts declaring proto2 and proto3 dependencies respectively. While the on-disk format of those versions is backwards-compatible with prior versions, the generated Java files may differ in incompatible ways. Users may therefore need to update their own Protocol Buffer dependencies to the new versions and regenerate any generated Java files created using prior versions.

### Newly Deprecated

The `KeySpacePath` class previously required an `FDBRecordContext` at object creation time. This was used to then resolve certain elements of the path and replace them with interned integers stored by the directory layer within the FoundationDB cluster. However, this meant that a `KeySpacePath` could not outlive a single transaction and therefore could not be reused when connecting to the same record store multiple times. To address this, `KeySpacePath`s should now be constructed without a transaction context, and new methods that do take an `FDBRecordContext` should be used to resolve a path into a subspace or tuple. The older constructor and methods have been deprecated.

Several constructors of the `RecordMetaDataBuilder` class have been deprecated. Users should transition to calling `RecordMetaData.newBuilder()` instead and calling the appropriate setter methods to specify the record descriptor or `MetaData` proto message. This new pattern allows for additional control as to which transitive dependencies of the record file descriptor are serialized into `MetaData` proto message as well as allowing for additional optional parameters to be added in the future in a backwards-compatible way.

### 2.4.36.0

* **Feature** A new RecordCursor API makes continuations less error-prone and safe use easier [(Issue #109)](https://github.com/FoundationDB/fdb-record-layer/issues/109)
* All changes from version [2.4.35.11](#243511)

### 2.4.35.12

* **Bug fix** The visibility of the `PathValue` class has been increased to `public` to match the visibility of the `resolveAsync` method of `KeySpacePath` [(Issue #266)](https://github.com/FoundationDB/fdb-record-layer/issues/266)

### 2.4.35.11

* **Bug fix** The FlatMapPipelinedCursor could return a null continuation when the inner cursor was done but was not ready immediately [(Issue #255)](https://github.com/FoundationDB/fdb-record-layer/issues/255)
* **Feature** The `RecordMetaDataBuilder` now performs additional validation by default before returning a `RecordMetaData` [(Issue #201)](https://github.com/FoundationDB/fdb-record-layer/issues/201)
* **Feature** Including a repeated field within a meta-data's union descriptor now results in an explicit error [(Issue #237)](https://github.com/FoundationDB/fdb-record-layer/issues/237)
* All changes from version [2.3.32.10](#233210)

### 2.4.35.0

* **Feature** The experimental `SizeStatisticsCollector` class was added for collecting statistics about record stores and indexes [(Issue #149)](https://github.com/foundationdb/fdb-record-layer/issues/149)
* **Feature** Key expressions were added for accessing elements of Protocol Buffer map fields [(Issue #89)](https://github.com/FoundationDB/fdb-record-layer/issues/89)
* **Feature** `MetaData` proto messages can now specify transitive dependencies and therefore support self-contained meta-data definitions [(Issue #114)](https://github.com/FoundationDB/fdb-record-layer/issues/114)
* **Feature** Appropriate covering indexes will now be chosen when specified with concatenated fields [(Issue #212)](https://github.com/FoundationDB/fdb-record-layer/issues/212)
* **Feature** The `KeySpacePath` class no longer includes a transaction object and old code paths are deprecated [(Issue #151)](https://github.com/FoundationDB/fdb-record-layer/issues/151)
* All changes from versions [2.3.32.8](#23328) and [2.3.32.9](#23329)

### 2.4.34.0

* **Feature** `KeySpacePath`s now support listing selected ranges [(Issue #199)](https://github.com/foundationdb/fdb-record-layer/issues/199)

### 2.4.33.0

* **Breaking change** Protobuf dependencies updated to versions 2.5.0 and 3.6.1 [(PR #251)](https://github.com/FoundationDB/fdb-record-layer/pull/215)
* **Breaking change** Support removed for migrating from the global directory layer [(Issue #202)](https://github.com/foundationdb/fdb-record-layer/issues/202)

## 2.3

### Features

The behavior of creating a new record store and opening it with the `createOrOpen` method has been made safer in this version. Previously, a record store that had not been properly initialized may be missing header information describing, for example, what meta-data and format version was used when the store was last opened. This meant that during format version upgrades, a store might be upgraded to a newer format version without going through the proper upgrade procedure. By default, `createOrOpen` now will throw an error if it finds a record store with data but no header, which is indicative of this kind of error. This should protect users from then writing corrupt data to the store and more easily detect these kinds of inconsistencies.

### Breaking Changes

The `OnlineIndexBuilder` has been renamed to the `OnlineIndexer`. This was to accommodate a new builder class for the `OnlineIndexer` as its constructors had become somewhat cumbersome. Users should be able to change the name of that class where referenced and to react to this change.

### Newly Deprecated

As the `Index` class now tracks the created and last modified version separately, the `getVersion` method has been deprecated. Users should switch to using `getAddedVersion` and `getLastModifiedVersion` as appropriate.

### 2.3.32.13

* **Bug fix** The `UnorderedUnionCursor` now continues returning results until all children have hit a limit or are exhausted [(Issue #332)](https://github.com/FoundationDB/fdb-record-layer/issues/332)

### 2.3.32.10

* **Bug fix** The FlatMapPipelinedCursor could return a null continuation when the inner cursor was done but was not ready immediately [(Issue #255)](https://github.com/FoundationDB/fdb-record-layer/issues/255)
* **Feature** Accessing a non-empty record store without its header now throws an error by default [(Issue #257)](https://github.com/FoundationDB/fdb-record-layer/issues/257)

### 2.3.32.9

* **Bug fix** The `FlatMapPipelinedCursor` could return a null continuation when the outer cursor returned no results [(Issue #240)](https://github.com/FoundationDB/fdb-record-layer/issues/240)
* **Bug fix** The `IntersectionCursor` could return a null continuation if a child cursor hit an in-band limit at the wrong time [(Issue #241)](https://github.com/FoundationDB/fdb-record-layer/issues/114)

### 2.3.32.8

* All changes from version [2.2.29.7](#22297)

### 2.3.32.0

* **Feature** Indexes and FormerIndexes now remember additional meta-data about when an index was created or last modified and what its name was [(Issue #103)](https://github.com/FoundationDB/fdb-record-layer/issues/103)

### 2.3.30.0

* **Bug fix** The `MetaDataValidator` now validates the primary key expression of each record type [(Issue #188)](https://github.com/FoundationDB/fdb-record-layer/issues/188)
* **Feature** The OnlineIndexer class now utilizes a builder pattern to make setting up index rebuilds more straightforward [(Issue #147)](https://github.com/FoundationDB/fdb-record-layer/issues/147)
* **Feature** The `FDBRecordStore` class now has a method for checking whether with a given primary key exists in the record store [(Issue #196)](https://github.com/FoundationDB/fdb-record-layer/issues/196)
* **Feature** The FDBReverseDirectoryCache now logs its metrics [(Issue #12)](https://github.com/FoundationDB/fdb-record-layer/issues/12)

## 2.2

### Features

The `FDBRecordStore` class now has additional methods for saving records that can check pre-conditions about whether a record with the same primary key as the new record does or does not already exist in the record store before saving. For example, the `insertRecord` method will throw an error if there is already a record present while `updateRecord` will throw an error if there is not already a record present (or if the type changes). Users are encouraged to switch to these methods to avoid accidentally deleting data if they can reason about whether a record does or does not exist prior to saving it. There are asynchronous variants of all of the new methods, and the existing `saveRecord` method remains available without any changes.

A new query plan, the `RecordQueryUnorderedUnionPlan`, allows the planner to produce union plans with “incompatibly ordered” subplans. In particular, the planner was previously only able of producing a union plan if all of its cursors were guaranteed to return records in the same order. For example, if there were one index on some field `a` of some record type and another on field `b` of that type, then the query planner would produce a union plan if the query were of the form:

```java
Query.or(Query.field("a").equalsValue(value1), Query.field("b").equalsValue(value2))
```

(In particular, it would plan two index scans over the indexes on `a` and `b` and then take their union. Each index scan returned results ordered by the primary key of the records, so the subplans were “compatibly ordered”.)  However, it could *not* handle a query like this with a union:

```java
Query.or(Query.field("a").lessThan(value1), Query.field("b").greaterThan(value2))
```

This would instead result in a plan that scanned the full record store. (In this case, the two candidate index scans would each return records in a different order—one index scan by the value of the field `a` and the other index scan by the value of the field `b`.) The new query plan can handle subplans that return results in any order and essentially returns results as soon as any subplan returns a result. The trade-off is that unlike a `RecordQueryUnionPlan`, the `RecordQueryUnorderedUnionPlan `does *not* remove duplicates. This means that: (1) if the query is run with the `setRemoveDuplicates` flag set to `true` (as it is by default) then there will almost certainly be a `RecordQueryUnorderedPrimaryKeyDistinctPlan` as part of the query plan (which is more memory intensive) and (2) duplicates are never removed across continuation boundaries.

### Breaking Changes

Index options have been refactored into their own class (appropriately titled the `IndexOptions` class). Option names and their semantics were preserved, but users who referenced the options via the constants in the `Index` class should transition their code to reference them through the `IndexOptions` class.

### 2.2.29.7

* **Bug fix** The `FlatMapPipelinedCursor` could return a null continuation when the inner cursor completed for an in-band reason even when not exhausted [(Issue #222)](https://github.com/FoundationDB/fdb-record-layer/issues/222)

### 2.2.29.0

* **Bug fix** Scans by record type key were sometimes preferred by the planner in inappropriate circumstances [(Issue #191)](https://github.com/FoundationDB/fdb-record-layer/issues/191)

### 2.2.28.0

* **Feature** A new union cursor and plan allows for incompatibly ordered plans to be executed without a full table scan [(Issue #148)](https://github.com/FoundationDB/fdb-record-layer/issues/148)

### 2.2.27.0

* **Feature** The logging level for text indexing was decreased from INFO to DEBUG [(Issue #183)](https://github.com/FoundationDB/fdb-record-layer/issues/183)

### 2.2.26.0

* **Peformance** Get range queries for unsplitting records in single-record requests are now done at the `WANT_ALL` streaming mode [(Issue #181)](https://github.com/FoundationDB/fdb-record-layer/issues/181)
* **Feature** Variants of the saveRecord method were added to the FDBRecordStore class to check whether records exist before saving a new record [(Issue #115)](https://github.com/FoundationDB/fdb-record-layer/issues/115)

### 2.2.25.0

* **Feature** A streaming mode can now be given to the `ScanProperties`  object so that users can optimize certain queries [(Issue #173)](https://github.com/FoundationDB/fdb-record-layer/issues/173)
* **Feature** Additional methods on the RecordMetaData class allow the user to supply strings for record type names [(Issue #157)](https://github.com/FoundationDB/fdb-record-layer/issues/157)
* **Feature** Constants defined for index options have been moved from the Index to IndexOptions class [(Issue #134)](https://github.com/FoundationDB/fdb-record-layer/issues/134)
* **Feature** The Record Layer can now be built and run using Java 11 [(Issue #142)](https://github.com/FoundationDB/fdb-record-layer/issues/142)
* **Feature** A text index can now be configured not to write position lists to save space [(Issue #144)](https://github.com/FoundationDB/fdb-record-layer/issues/144)
* **Feature** The number of levels used within a `RankedSet`'s skip list can now be configured [(Issue #95)](https://github.com/FoundationDB/fdb-record-layer/issues/95)

## 2.1

### Features

A new key expression type on the “record type” of a record allows the user to lay their data out in a way that is more akin to the way tables are laid out in a traditional relational database. In particular, if the user prefixes all primary keys with this expression type, then each record type will live in contiguous segments within the database. This can speed up index build times (as only the range containing the records of the corresponding time needs to be scanned) and improve query performance without secondary indexes. As part of this work, the user can specify a shortened “record type key” as part of the record type definition so that the cost is typically only two bytes per key.

The capability and reliability of text queries on more sophisticated indexes has been greatly improved. In particular, it allows for the user to specify queries on a map field that query on the value of portion of the map (when grouped by key) as well as choosing to produce a covering plan if the text index covers all of the required fields of a given query. Queries can also be more reliably combined with other predicates through an “and” or “or” clause.

### 2.1.23.0

* **Performance** The RankedSet data structure now adds fewer conflict ranges by being more selective about initializing each level [(Issue #124)](https://github.com/FoundationDB/fdb-record-layer/issues/124)

### 2.1.22.0

* **Performance** The RankedSet data structure now adds fewer conflict ranges when inserting or removing elements from the structure [(Issue #122](https://github.com/FoundationDB/fdb-record-layer/issues/122)[)](https://github.com/FoundationDB/fdb-record-layer/issues/122)
* **Feature** The Record Layer now publishes a second set of artifacts which shade their guava and Protocol Buffer dependencies [(Issue #73)](https://github.com/FoundationDB/fdb-record-layer/issues/73)

### 2.1.21.0

* **Feature** Queries can now ask whether a nested message field is null [(Issue #136)](https://github.com/FoundationDB/fdb-record-layer/issues/136)

### 2.1.20.0

* **Performance** “Or” predicates can now be used with text queries without always reverting to a full scan of the record store [(Issue #19)](https://github.com/FoundationDB/fdb-record-layer/issues/19)

### 2.1.19.0

* **Bug fix** Multi-type text indexes now add type predicates if the query only requires a subset of the types on which the index is defined [(Issue #126)](https://github.com/FoundationDB/fdb-record-layer/issues/126)
* **Bug fix** Combining a “not” predicate with a text query would previously throw an error during planning [(Issue #127)](https://github.com/FoundationDB/fdb-record-layer/issues/127)
* All changes from version [2.1.14.6](#21146)

### 2.1.18.0

* **Feature** Add a strict compatibility mode for the query-planning `IndexScanPreference` option [(Issue #119)](https://github.com/FoundationDB/fdb-record-layer/issues/119)

### 2.1.17.0

* **Bug fix** Certain predicates could result in an error to be thrown during planning when combined with text index scans [(Issue #117)](https://github.com/FoundationDB/fdb-record-layer/issues/117)

### 2.1.15.0

* **Feature** Text indexes now support querying the values of maps [(Issue #24)](https://github.com/FoundationDB/fdb-record-layer/issues/24)
* **Bug fix** The FDBDatabase class now correctly threads through the datacenter ID if specified [(Issue #106)](https://github.com/FoundationDB/fdb-record-layer/issues/106)

### 2.1.14.6

* **Feature** The time window leaderboard index now emits additional KPIs [(Issue #105)](https://github.com/FoundationDB/fdb-record-layer/issues/105)
* **Feature** Add a strict compatibility mode for the query-planning `IndexScanPreference` option [(Issue #119)](https://github.com/FoundationDB/fdb-record-layer/issues/119)

### 2.1.14.0

* **Bug fix** The planner previously would chose to use a FanOut index even if this could result in missing or duplicated entries in certain circumstances [(Issue #81)](https://github.com/FoundationDB/fdb-record-layer/issues/81)
* **Feature** Covering index plans have a more human-readable string representation [(PR #77)](https://github.com/FoundationDB/fdb-record-layer/pull/77)

### 2.1.13.0

* **Bug fix** A `FanOut` index could have been chosen by the query planner if no other index existed instead of a record scan even though that could have resulted in missing results [(Issue #75)](https://github.com/FoundationDB/fdb-record-layer/issues/75)

### 2.1.12.0

* **Feature** Text indexes can now be included as part of covering index plans if all required results in the query are covered by the index [(Issue #25)](https://github.com/FoundationDB/fdb-record-layer/issues/25)

### 2.1.11.0

* **Feature** Records can now be preloaded and cached to acheive better pipelining in otherwise blocking contexts [(PR #72)](https://github.com/FoundationDB/fdb-record-layer/pull/72)

### 2.1.10.0

* **Feature** A new record type key expression allows for structuring data in a record store more akin to how tables are stored in a traditional relational database [(Issue #27)](https://github.com/FoundationDB/fdb-record-layer/issues/27)
