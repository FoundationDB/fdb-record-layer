# Release Notes

This document contains a log of changes to the FoundationDB Record Layer. It aims to include mostly user-visible changes or improvements. Within each minor release, larger or more involved changes are highlighted first before detailing the changes that were included in each build or patch version. Users should especially take note of any breaking changes or special upgrade instructions which should always be included as a preface to the minor version as a whole before looking at changes at a version-by-version level.

As the [versioning guide](Versioning.md) details, it cannot always be determined solely by looking at the version numbers whether one Record Layer version contains all changes included in another. In particular, bug fixes and backwards-compatible changes might be back-ported to or introduced as patches against older versions. To track when a patch version has been included in the master release train, some releases will say as a note that they contain all changes from a specific patch.

## 2.7

# Breaking Changes

The Guava version has been updated to version 27. Users of the [shaded variants](Shaded.html#Variants) of the Record Layer dependencies should not be affected by this change. Users of the unshaded variant using an older version of Guava as part of their own project may find that their own Guava dependency is updated automatically by their dependency manager.

<!--
// begin next release
### NEXT_RELEASE

* **Bug fix** `OnlineIndexer.buildEndpoints` should not decrease the scan limits on conflicts [(Issue #511)](https://github.com/FoundationDB/fdb-record-layer/issues/511)
* **Bug fix** `KeyValueCursor.Builder.build` now requires `subspace`, `context` and `scanProperties` are specifed, throws RecordCoreException if not [(Issue #418)](https://github.com/FoundationDB/fdb-record-layer/issues/418)
* **Bug fix** Fix 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Added a counter counting indexes that need to be rebuilt and changed related logging to `DEBUG` [(Issue #604)](https://github.com/FoundationDB/fdb-record-layer/issues/604)
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

