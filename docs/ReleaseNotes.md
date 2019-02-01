# Release Notes

This document contains a log of changes to the FoundationDB Record Layer. It aims to include mostly user-visible changes or improvements. Within each minor release, larger or more involved changes are highlighted first before detailing the changes that were included in each build or patch version. Users should especially take note of any breaking changes or special upgrade instructions which should always be included as a preface to the minor version as a whole before looking at changes at a version-by-version level.

As the [versioning guide](Versioning.md) details, it cannot always be determined solely by looking at the version numbers whether one Record Layer version contains all changes included in another. In particular, bug fixes and backwards-compatible changes might be back-ported to or introduced as patches against older versions. To track when a patch version has been included in the master release train, some releases will say as a note that they contain all changes from a specific patch.

<!--
// begin template
### NEXT_RELEASE

* **Bug fix** Fix 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
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

// end template
-->

## 2.5

### Breaking changes

In order to simplify typed record stores, the `FDBRecordStoreBase` class was turned into an interface and all I/O was placed in the `FDBRecordStore` class. This makes the `FDBTypedRecordStore` serve as only a type-safe wrapper around the `FDBRecordStore` class. As part of this work, the `IndexMaintainer` interface and its implementations lost their type parameters and now only interact with `Message` types, and the `OnlineIndexerBase` class was removed. Additionally, the `FDBEvaluationContext` class was removed in favor of using `EvaluationContext` (without a type parameter) directly. That class also no longer carries around an `FDBRecordStore` reference, so query plans now require an explicit record store in addition to an evaluation context. Finally, the `evaluate` family of methods on key expressions no longer take evaluation contexts. Users should switch any uses of the `OnlineIndexerBase` to a generic `OnlineIndexer` and will need to update any explicit key expression evluation calls. Users should also switch from calling `RecordQueryPlan::execute` to calling `FDBRecordStore::executeQuery` if possible as that second API is more stable (and was not changed as part of the recent work).

<!--
// begin next release
### NEXT_RELEASE

* **Bug fix** Directory layer cache size was accidentally defaulted to zero [(Issue #251)](https://github.com/FoundationDB/fdb-record-layer/issues/251)
* **Bug fix** Fix 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Remove blocking calls from LocatableResolver constructors [(Issue #261)](https://github.com/FoundationDB/fdb-record-layer/issues/261)
* **Performance** Improvement 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Tracing diagnostics to detect blocking calls in an asynchronous context [(Issue #262)](https://github.com/FoundationDB/fdb-record-layer/issues/262)
* **Feature** New limit on the number of bytes scanned while executing a cursor [(Issue #349)](https://github.com/FoundationDB/fdb-record-layer/issues/349)
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

### Breaking changes

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

