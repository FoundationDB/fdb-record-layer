# FoundationDB Record Layer Overview

Records are instances of Protobuf messages. The core layer conventionalizes serialization
and maintains secondary indexes. It supports simple predicate queries that use those indexes.

Each record store is an extent of records described by meta-data. The extent can
include records of multiple types, unlike (most) relational databases.
Since they are stored together, all record types must have compatible primary keys.

The meta-data contains:
* Record type descriptors, which are just Protobuf message descriptors.
* Record index definitions
  * A record index is an ordered list of expressions that are evaluated against the record
    to produce the index key value.
  * Index expressions can be comprised of fields of the record or its subrecords, or
    the result of the application functions to a record, or a combination.
  * There is a special primary key index. The actual record is stored under this key. That
    is, it is a clustered index.

## Creating a Record Store

An `FDBRecordStore` requires:
* A `FDBRecordContext`, a thin transaction wrapper, gotten from an `FDBDatabase` instance with `openContext()`.
* A path for storage (a `Subspace` or a `KeySpacePath`). It is up to the application to make this unique.
* A `RecordMetaDataProvider` for `RecordMetaData` describing the records to be stored there.

The simplest way to make `RecordMetaData` is from a Google Protobuf `Descriptors.FileDescriptor`,
such as is generated statically by `protoc`. Single field indexes can be declared inline using extension options.

```protobuf
import "record_metadata_options.proto";

message MySimpleRecord {
  required int64 rec_no = 1 [(com.apple.foundationdb.record.field).primary_key = true];
  optional string str_value_indexed = 2 [(com.apple.foundationdb.record.field).index = {}];
  optional int32 num_value_unique = 3 [(com.apple.foundationdb.record.field).index = { unique: true }];
  optional int32 num_value_2 = 4;
  optional int32 num_value_3_indexed = 5 [(com.apple.foundationdb.record.field).index = {}];
}
```
Note that at the moment, we do not support unsigned types (i.e., `uint32`, `uint64`, `fixed32`, and `fixed64`) for fields
and attempting to use one will result in an `MetadataDataException` being thrown when one attempts to load the
metadata.

You will also need to have a union descriptor that contains all your top level record types (even if you only have one), you can either declare this with an option

```protobuf
message UnionDescriptor {
  option (com.apple.foundationdb.record.record).usage = UNION;
  optional MySimpleRecord _MySimpleRecord = 1;
}
```

or, if you can't import the options proto, by naming it the right thing:

```protobuf
message RecordTypeUnion {
  optional MySimpleRecord _MySimpleRecord = 1;
}
```

Then build the meta-data:

```java
RecordMetaData metaData = RecordMetaData.build(TestRecords2Proto.getDescriptor());
```

If you need more complicated secondary indexes or primary keys, the more verbose and powerful builder structure is required:
```java
RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords3Proto.getDescriptor());
metaDataBuilder.getRecordType("MyHierarchicalRecord").setPrimaryKey(
        concatenateFields("parent_path", "child_name"));
metaDataBuilder.addIndex(metaDataBuilder.getRecordType("RestaurantRecord"),
        new Index("review_rating",
                field("reviews", FanType.FanOut).nest("rating")));
RecordMetaData metaData = metaDataBuilder.getRecordMetaData();
```

### Primary Keys

Primary keys are just a special kind of index that says where to store the whole record. Primary keys are required for any record that you want to store, that isn't nested in another record.
Primary keys use the same data structures as secondary indexes, as defined in the next section, except they  *must* result in a single value, so you cannot use `FanType.FanOut` on repeated values.
If you create an index (any index) that is not a primary key, that is a secondary index.
Primary keys are a mapping from the result of evaluating the expression, to the rest (or all) of the record. Secondary Indexes concatenate the primary key onto the end of the result of the expression, and map to nothing.


## Secondary indexes

A lot of indexes that we want are more complicated than a simple field reference.
Each key in the index is a list of individual elements. An expression is a function that converts a record into a list of keys.

* *field*: Get that field from the Record required.
* *nest*: Requires the calling field to be a message, not a primitive type. This will grab that message's nested field. Also takes any other expression.
* *concat*: Append different values to each other, as separate elements. fanout will be preserved. Note: concat within a nest appears side by side with everything else, but FanType.Concatenate creates a nested list. This is to ensure that the index in the list always corresponds to a single expression.

* *FanType.FanOut*: _requires repeated_ Causes multiple keys to be created, one for each value of the field
* *FanType.Concatenate*: _requires repeated_ Causes multiple values to be concatenated into a nested tuple within a single element
* *FanType.None*: _default_  _requires not repeated_ Just uses that value. Since the field is a scalar, no special logic is needed.

### Basic examples

Let's start out with a message:

```protobuf
message Record {
    optional string a=2;
    optional string b=3;
}
```

This allows for the following indexes:

```java
field("a");
field("b");
concat(field("a"), field("b"));
concat(field("b"), field("a"));
```
Now if we have the following record:

```
{a=x, b=y}
```

Those indexes each produce one key for that record (respectively):

```
[x]
[y]
[x, y]
[y, x]
```

Now let's throw a repeated value in there:

```protobuf
message Record {
    repeated string a=2;
    optional string b=3;
}
```

This means that all those `field("a")` calls must either have `FanType.FanOut` or `FanType.Concatenate`

With the value `{a=x1, a=x2, b=y}`:

* `field("a", FanType.Concatenate)`
  Produces one key, with one element: `[[x1, x2]]`
* `field("a", FanType.FanOut)`
  Produces two keys: `[x1]` and `[x2]`
* `concat(field("a", FanType.Concatenate), field("b"))`
  Produces one key: `[[x1, x2], y]`
* `concat(field("a", FanType.FanOut), field("b"))`
  Produces two keys: `[x1, y]` and `[x2, y]`
* `concat(field("b"), field("a", FanType.FanOut))`
  Produces two keys: `[y, x1]` and `[y, x2]`
* `field("b")`
  Produces one key: `[y]`

If `b` were also repeated, and we had the record `{a=x1, a=x2, b=y1, b=y2}`:

* `concat(field("a", FanType.FanOut), field("b", FanType.FanOut))` Produces four keys:
    * `[x1, y1]`
    * `[x1, y2]`
    * `[x2, y1]`
    * `[x2, y2]`

### Nesting

Lets say you have a schema:

```protobuf
message Car {
    required string id = 1;
    repeated Seat s = 2;
}
message Seat {
    optional string back = 1;
    optional string seat = 2;
    repeated string armrest = 3;
}
```

You could create the following indexes (and others)

* `field("s", FanType.FanOut).nest("back")`
* `field("s", FanType.FanOut).nest(concat(field("back"), field("seat"), field("armrest", FanType.Concatenate)))`

With the following value:

* `{id="car1", s=[{back="red1", seat="red2"}, {back="blue1", seat="blue2", armrest=["a", "b", "c"]}]}`

You would get the following sets of index keys:

* `field("s", FanType.FanOut).nest("back")`
    * `["red1"]` 
    * `["blue1"]`
* `field("s", FanType.FanOut).nest(concat(field("back"), field("seat"), field("armrest", FanType.Concatenate)))`
    * `["red1", "red2", null]`
    * `["blue1", "blue2", ["a", "b", "c"]]`

## Persistent Meta-data

If record meta-data for the core layer is generated from application meta-data, it may be
more convenient to generate it occasionally and persist it, rather than generating it
again for every query.

`FDBMetaDataStore` stores an instance of `RecordMetaDataProto.MetaData`, a protobuf record
description for `RecordMetaData`, using `saveRecordMetaData()`, and then implements
`RecordMetaDataProvider`. Using `RecordMetaDataProto.MetaData` builders to construct the
meta-data is then up to the application.

## Storing a Record

```java
    final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase(clusterFile);
    try (FDBRecordContext context = database.openContext()) {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FDBRecordStore recordStore = FDBRecordStore.newBuilder()
            .setMetaDataProvider(metaData)
            .setContext(context)
            .setSubspace(new Subspace(Tuple.from("mydb", "mytest").pack()))
            .build();
        recordStore.deleteAllRecords();
        TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
        recBuilder.setRecNo(1);
        recBuilder.setStrValue("abc");
        recBuilder.setNumValue(123);
        recordStore.saveRecord(recBuilder.build());
        context.commit();
    }
```

You must call `commit()` before closing for changes to be saved. Commit can only be called once 
per `FDBRecordContext` and cannot be called after the context is closed.

## Retrieving a Record by Primary Key

```java
    try (...) {
        ...
        FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
        TestRecords1Proto.MySimpleRecord.Builder myrec1 = TestRecords1Proto.MySimpleRecord.newBuilder();
        myrec1.mergeFrom(rec1.getRecord());
    }
```

## Querying for Records

Queries are defined starting from the `RecordQuery` class, allowing you to define the criteria by which records are to be matched
and output sort order, for example:
```java
    RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("myRecord")
        .setFilter(Query.and(
            Query.field("str_field").equalsValue("hello!"),
            Query.field("num_field").greaterThan(10)))
        .setSort(Key.Expressions.field("rec_no"))
        .build();
```

Filters can be applied on fields of nested record types with the `.matches()` filter, such as:

```java
    Query.field("child_record").matches(
        Query.field("child_field1").equalsValue("foo"));
```

In the case of repeated fields `oneOfThem()` may be used to apply a query predicate that stops
at the first match, such as:

```java
    Query.field("repeated_str").oneOfThem().equalsValue("Mary Smith");
```

`RecordStore.executeQuery()` generates a plan for the query and executes it, returning a 
stream of records according to the provided query:

```java
    try (...) {
        ...

        RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("abc"))
                .build();

        try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(query)) {
            while (cursor.hasNext()) {
                FDBQueriedRecord<Message> rec = cursor.next();
                TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
            }
         }
    }
```

## Sorting

Sorting over repeated fields is defined by providing a `FanType`, in a similar fashion as it is used when 
defining indexes, only with slightly different implications. 

For example, given the following three records:

    r1.f = ["aaa", "bbb"]
    r2.f = ["aaa", "ccc"]
    r3.f = ["brr", "cxx"]

`FanType.Concatenate` will sort by the whole value so you will get back:

    [r1, r2, r3]

`FanType.FanOut` will sort based on a single value, so you'll get back: 

    [r1, r2, r1, r3, r2, r3]

If you call `RecordQueryPlanner.plan` on a `RecordQuery` with `removeDuplicates=true`, it will remove the duplicates within a single continuation, so if you used the cursor to get the first two, you would get `[r1, r2]`, and then if you used the continuation, you'd get back `[r3, r2]`. Providing `null` for the sort will just cause the results to come back in whatever is most efficient for the query.

## Indexing by Rank

A special kind of index is supported that implements efficient access to an ordering by some field(s) according to the ordinal position in that order. This can be used to implement leaderboards. For example,
* Get the rank for a record that contains a score value.
* Get records whose rank is between two bounds.

For a single field, this can be done by choosing the `RANK` index type.

```protobuf
  optional int32 score = 2 [(field).index = { type: "rank" }];
```

A query specifies a predicate on `Query.rank`:

```java
  RecordQuery query = RecordQuery.newBuilder().setRecordType("MyRankedRecord").setFilter(Query.rank("score").equalsValue(10L)).build();
```

The index can be segregated by some key fields, giving separate rankings for every combination of those keys. This is done by building the index on a grouped perspective of the score field(s). By default, exactly the rightmost key field is the score and the remainder to its left are the grouping keys. To include more fields in the score key, use `groupBy(n)` on the key with the count.

It is also possible to specify the score and grouping keys separately:

```java
    new Index("rank_per_game", Key.Expressions.field("score").groupBy(Key.Expressions.field("game_id")), IndexTypes.RANK)
```

The rank itself is not stored in the record, because it is dynamic. To get the current rank for an indexed score, give the same field as used to build the index (simple or grouped) to evaluation as a special function:

```java
  recordStore.evaluateRecordFunction(Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("game_id"))), rec)
```

## Indexing by Version

Another special kind of index allows records to be indexed in a way that orders them by their most recent update time which allows for efficiently querying for changes, for example. This is done by associating with each record a monotonically increasing version number that is unique per record (by default), though one can supply one's own version if one likes. Unlike other index types which are based entirely off of information within the protobuf message, this kind of index will add information to the record at commit time to the underlying data store. To use this kind of index, one has to activate the `stores_record_versions` option within the schema definition file:

```protobuf
  option (schema).store_record_versions = true;
```

Alternatively, one can call `setStoreRecordVersions` when building the meta-data using a `RecordMetaDataBuilder`:

```java
   metaDataBuilder.setStoreRecordVersions(true);
```

If one does this to the metadata of a live database, then records that were written before the option is set will not be assigned a version, but they will sort before newer records in any index by version (with the value `null` standing in for the version). This will cause each record insertion to also log at what version each record is written. This also causes `QueriedRecord`s and `StoredRecord`s, when retrieved from the record store, to set their version field. This means if one wants to know the last version that a specific record with primary key `primaryKey` was written, one can access that with:

```java
  RecordVersion version = recordStore.loadRecord(primaryKey).getVersion();
```

Or:

```java
  RecordVersion version = recordStore.loadRecordVersion(primaryKey).orElseThrow(() -> new IllegalStateException("Missing version!"));
```

One can then define an index of type where the key expression contains exactly one column whose value is of type `Key.Expressions.Version` and which has the `VERSION` index type. For example, it might look like:

```java
  metaDataBuilder.addIndex(metaDataBuilder.getRecordType("MySimpleRecord"),
        new Index("MySimpleRecord$num2-version", Key.Expressions.concat(Key.Expressions.field("num_value_2"), KeyExpressions.VERSION), IndexTypes.VERSION));
  metaDataBuilder.addIndex(null,
        new Index("globalVersion", KeyExpressios.VERSION, IndexTypes.VERSION));
```

Records can then by queried by version or sorted by version efficiently. A sort by version might look like this:

```java
  RecordQuery query = RecordQuery.newBuilder().setSort(KeyExpressions.VERSION).build();
```

When executed, this will return the list of records currently in the record store with the oldest records first. (Records that have been deleted will not be returned, and records that have been inserted multiple times will be returned as if they had only been inserted the last time they had been updated.) If one already has a record version `previousVersion`, indicating, e.g., the last time one had polled the database, one can get records inserted since then with a query like this:

```java
  RecordQuery query = RecordQuery.newBuilder().setFilter(Query.version().greaterThan(previousVersion)).build();
```

One can combine these queries in the usual ways with other predicates if one wants to, for example, only look at updates since `previousVersion` to certain records. For example, to only get records where the field `num_value_2` is equal to 42, the query might look like this:

```java
  RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("MySimpleRecord")
        .setFilter(Query.and(Query.version().greaterThan(previousVersion), Query.field("num_value_2").equalsValue(42)))
        .setSort(KeyExpressions.VERSION)
        .build();
```

Keep in mind that deletes will not show up in the index, so if one needs to know if a record has been removed from the record store since the last time one has polled (if one were using this feature to keep two record stores in sync, for example), one needs to insert some kind of tombstone record upon deletion. It is up to client code to come up with what tombstones might look like for their use cases.

## Indexing and Querying of missing / null values

By default, a field that is not present in a record is treated as a special `null` value, like in SQL.
* If the field is indexed, using `scanIndex` to get an `IndexEntry` will find a `null` in the corresponding `Tuple` element.
* If the field is part of the primary key, a `null` will appear in the `FDBStoredRecord.getPrimaryKey` element.
* Queries implement tri-valued logic for predicates on nullable fields:
  * Comparison with `null` is UNKNOWN (also represented by `null`).
  * `Query.not` of UNKNOWN is still UNKNOWN.
  * `Query.and` is UNKNOWN when none of the conjuncts is FALSE and any of the conjuncts is UNKNOWN.
  * `Query.or` is UNKNOWN when none of the disjuncts is TRUE and any of the disjuncts is UNKNOWN.
* `Query.field("x").isNull()` and `.notNull()` must be used instead to get successful matches.
* Sorting puts `null` values at the beginning, before any non-`null` values.
* Unique indexes optionally allow multiple entries for the null value without a uniqueness violation.

In Protobuf version 2, `Message.hasField()` tracks whether a field was present in the serialized form when it was loaded from the database and whether it was set through a `Builder` when it was first constructed.

In Protobuf version 3, compatible behavior is selected by specifying
```
syntax = "proto2";
```
in the file and using the older syntax. Absent that, or if `proto3` is specified, `protoc`-generated Java `Message` classes do not maintain separate state for how fields were loaded / set. `hasField()` is `false` if the field has its default value and default values are not serialized to the database. `DynamicMessage` is the same: it varies its behavior based on `Descriptors.FileDescriptor.getSyntax()`. Unless that is `PROTO2`, `DynamicMessage.setField()` makes setting a scalar field to its default value equivalent to clearing the field. This also happens when parsing a serialized record, even if the bytes contain explicit default values written by Protobuf version 2.

This means that when using Protobuf version 3 an integer field that happens to be zero or a string field that happens to be the empty string will have all of the special `null` value behaviors listed above.

To disable this, instead of:
```
Key.Expressions.field("x")
```
use:
```
Key.Expressions.field("x", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NOT_NULL)
```
It will still not be possible to tell whether the field was set when built / stored in the database, but zero and the empty string will not be special.

To support compatible special handling of nulls in Protobuf version 3, special messages are defined in `tuple_fields.proto`, such as `NullableInt32`. If the message field is not set, the field will have the special `null` value. If the message field is set, even if no fields are set in the message, the field has the ordinary scalar value for purposes of indexing and queries.

It is also possible to get similar special handling of nulls by wrapping the scalar value in a user-defined message:
```
message MyNullableInt32 {
  option (com.apple.foundationdb.record.record).usage = NESTED;
  int32 value = 1;
}
```
and then using a nested field in place of a simple field:
```
Key.Expressions.field("x")
    .nest(Key.Expressions.field("value", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NOT_NULL))
```
Note that the parent field has the default null behavior and the child suppresses null checking. This works because message fields still have reliable `hasField()` behavior and because a zero will only be stored in the child (and treated as an ordinary value, not a nested null) when the logical field is present.

## Custom secondary indexes

It is possible to define a custom secondary index of a type that is loaded by name from the application's classpath (see `IndexMaintainer`). 
These indexes will not be recognized or used by the planner, however they may be scanned directly. Note that details of the maintainer implementation 
APIs are likely to change.

