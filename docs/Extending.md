# Extending the Record Layer

As the name suggests, the Record Layer is meant to sit above the FoundationDB key-value store and possibly below higher level layers such as query language parsers. Within the layer itself, there are a number of extension points. Generally, concepts that have several variants or involve significant implementation tradeoffs are defined and accessed through such an extension point.

Objects that need to be known by the Record Layer core are generally discovered using `ServiceLoader`. This allows them to be introduced by additional .jar files in the class path.

**NOTE**: These extension interfaces have varying levels of maturity. Our `@API` stability annotations are meant to make this level of maturity clearer to users of the Record Layer. Anyone writing extensions for the Record Layer which, for whatever reason, are not appropriate to submit back to the core source, is still *strongly* encouraged to create GitHub issues or forum posts noting what they used so that their extensions can be better supported in a compatible way as the system evolves.

## Indexing data

A (secondary) index in the Record Layer is a subspace of the record store uniquely associated with the index. This subspace is updated when records are inserted or updated in the same transaction so that it is always consistent with the (primary) record data.

Within the meta-data, an index consists of:

* A unique **name**. The name is used for meta-data operations. There is a separate, more compact, unique identifier that is the actual part of the index's subspace prefix key.
* An index **type**. An index type is a unique string used to look up an *index maintainer* in a registry.
* A **key expression**. A key expression is evaluated against a stored record to produce a list of tuples for each index entry for the same record.
* Various **index options**. These options can tweak index behavior by, for example, adding uniqueness constraints.

The most basic index type is `value`. A value index follows the [secondary index pattern](https://apple.github.io/foundationdb/simple-indexes.html) in the FoundationDB samples more or less exactly: each index entry consists of a key-value pair within the index's subspace whose key suffix is the indexed value concatenated with the record's primary key. To find matching records as part of a query, a range scan is performed against the index's subspace with the target value as a prefix and the primary keys that follow it used to retrieve the actual records.

The most basic key expression type is `field`. It gets a field value from the record and returns a singleton list with the singleton tuple of that value. If the field were repeated, the list would be longer. If a `concat` expression were used, the tuple would be longer.

The first consideration in designing a new way of indexing data is therefore determining whether this is a new way of *storing* the data or just a new way of *extracting* the data from the record and then storing it in the standard way. In the former case, we require a new index maintainer. In the latter case, we require a new key expression. For example, the Record Layer's indexes on ordinal rank require a skip list optimized for FoundationDB's key space which is maintained by the `RankIndexMaintainer`. In contrast, collation indexes are maintained exactly like standard value indexes once the collation has been applied; this is handled by `CollateFunctionKeyExpression`.

An index maintainer is an implementation of the `IndexMaintainer` abstract class. The objects in the service loader are the `IndexMaintainerFactory` instances which are associated with one or more index types and produce index maintainers with specific state for each use. Most index maintainers extend `StandardIndexMaintainer`, which has methods for things like determining which index entries have in fact changed to avoid removing and then adding right back the same entry.

A key expression is an implementation of the `KeyExpression` interface. The most general form of a key expression requires changes to the core because it must implement `toProto` for serialization with the meta-data and be known to `fromProto` for the other direction.

A special kind of key expression meant specifically for implementations outside the core is a `FunctionKeyExpression`. These are produced by instances of `FunctionKeyExpression.Factory` found through the service loader. A function key expression adds:

* a **name**, through which the expression can be found and with which it can be stored in the meta-data's protobuf.
* an **argument**, another key expression, which is evaluated as part of evaluating the function key expression.

The `FunctionKeyExpression`'s `evaluateFunction` method is called with the record being saved, so it can do whatever it wants to get data from there. If the function just needs some field(s) from the records, it is a best practice to specify those as part of the argument expression.

## Record functions

A `RecordFunction` is evaluated by a record store against an `FDBRecord`. A function is identified by name.

A `StoreRecordFunction` is evaluated by the record store itself, so new ones can only be added by changes to the core or a new record store subclass.

An `IndexRecordFunction` is evaluated by an index maintainer. The `canEvaluateRecordFunction` method is used to choose the index maintainer and then `evaluateRecordFunction` to do the actual evaluation.

An example of a record function implemented by an index is `rank`. The index maintains a skip list of the indexed value at save time. Then, at query time, rank can be evaluated more efficiently. Something similar could be done with other data structures that store some kind of state from which relationships between a given record and the other index records can be calculated.

## Aggregate functions

An `IndexAggregateFunction` is like an aggregate function in SQL, but it is evaluated with the assistance of an index. The index maintains aggregates (or partial aggregates) so that it can efficiently calculate the aggregate without having to actually scan records. The index does not typically store enough information to associate individual records back with the aggregate data and so is more like a materialized view of an aggregate scan. An `IndexAggregateFunction` has an *operand*, which is like the `GROUP BY` expression in SQL, separating the aggregates into several bins.

An `IndexAggregateFunction` is evaluated by an index maintainer. The `canEvaluateAggregateFunction` method is used to choose the index maintainer and then `evaluateAggregateFunction` to do the actual evaluation.

An ordinary `value` index can implement the `min` and `max` aggregate functions. The index stores values in order (possibly grouped by earlier fields on a compound index). The min is just the smallest such value and the max the largest.

The remaining index aggregate functions in the core, such as `count` and `sum`, are implemented using FoundationDB's atomic mutations. This gives the added benefit that updates to the total do not cause transaction conflicts.

## Query components

A `QueryComponent` is a tri-valued (`true`, `false`, `unknown`, as in SQL) predicate used to implement filtering in queries. It is invoked with the record and a context of parameter bindings.

## Query execution plan elements

A query plan is a tree of `RecordQueryPlan`. The plan's execute method returns a `RecordCursor` of queried records. For the root of the tree, these records are the result of the query. For the leaves, the plan element will open a cursor that scans records from the record store, usually through a secondary index. The intermediate plan elements execute their children, take the cursors that result and filter, buffer, or combine them in some way.

A `RecordCursor` is an asynchronous form of iterator based on `CompletableFuture`. Execution of a tree of `RecordQueryPlan` produces a tree of `RecordCursor`. A new `RecordQueryPlan` implementation might require a new implementation of `RecordCursor`.

Since FoundationDB is optimized for throughput more than individual latency, it is important to keep a reasonable amount of database work outstanding. The core set of `RecordCursor` implementations include `mapPipelined` and `flatMapPipelined`, which turn input cursor elements into futures and sub-streams, respectively, keeping a specified number of elements outstanding ahead of where the cursor itself has generated results.

`RecordQueryPlan.execute` and `RecordCursor`s must also support *continuations*, which are opaque byte strings used to identify a position in the stream at which the cursor can continue again, possibly in a different transaction. Since all the normal leaf elements and cursors support continuations, implementing them for a new cursor usually means combining the continuation state of child cursors when generating a continuation and then splitting the combined continuation into initial continuations for the child sub-plans when resuming execution.

## Query planner rules

Making the query planner extensible is being tracked as [issue #274](https://github.com/FoundationDB/fdb-record-layer/issues/274).


