# Getting Started Guide

This is intended as a quick guide to getting started developing applications with the FDB Record Layer. We will cover:

* Getting the Record Layer artifacts
* Running FDB locally
* Declaring record types
* Defining indexes
* Performing record CRUD
* Querying records

To keep the guide simple, we will not cover:

* Modifying a schema
* Using advanced [index](Overview.md#secondary-indexes) and [query](Overview.md#querying-for-records) functionality

## Setup

Install Java JDK for version 8 or above; for this demo we are using JDK `1.8.0_181`.

Install FoundationDB [here](https://github.com/apple/foundationdb/releases).
Once it is installed you should verify that you can run FDB and connect to it using `fdbcli`:

```bash
$ fdbcli
Using cluster file `/usr/local/etc/foundationdb/fdb.cluster'.

The database is available.

Welcome to the fdbcli. For help, type `help'.
fdb>
```

Be sure you see the message `The database is available.` type `exit` to quit the `fdbcli` session:

```
fdb> exit
```

Let’s create a fresh demo project. For this example, we’ll be creating a basic Java app with Gradle:

```bash
$ mkdir record-layer-demo
$ cd record-layer-demo
$ gradle init --type basic
```
Gradle 'init' will ask you some questions on which DSL to use in build scripts,
whether to use new APIs and behaviors, and how you would like to name your project.
Choose `groovy`, `yes` to new APIs, and take the proffered project name `record-layer-demo`.

Now we should add the Record Layer as a dependency of our project. The Record Layer dependencies
are published to Maven Central, so we can declare the dependency by adding the following to our
project's `build.gradle` file:

```groovy
repositories {
    mavenCentral()
    maven {
      url "https://ossartifacts.jfrog.io/artifactory/fdb-record-layer/"
    }
}
dependencies {
    // gradle init put some other stuff here, too...
    implementation 'org.foundationdb:fdb-record-layer-core-pb3:VERSION_NUMBER'
}
```

Replace `VERSION_NUMBER` with a recent version of the artifact from Maven Central (See
[mvnrepository](https://mvnrepository.com/artifact/org.foundationdb/fdb-record-layer-core-pb3)
for a listing of published versions).

## Protobuf Configuration

Our sample project will use [Protocol Buffers](https://developers.google.com/protocol-buffers/) (protobuf)
to define our record meta-data. First, since we are using Gradle, let's include the
[protobuf-gradle-plugin](https://github.com/google/protobuf-gradle-plugin),
which will allow us to add protobuf compilation as a step in our build process.
Add this to the top of your `build.gradle`, ahead of the above `repositories` and
`dependencies` added above:

```groovy
plugins {
    id 'java'
    id 'com.google.protobuf' version "0.8.19"
}
```
Gradle complains the `java` plugin must be included for the protobuf plugin to run so also
include `id'java'`.

Additionally, add the following:

```groovy
protobuf {
    generatedFilesBaseDir = "${projectDir}/src/main/generated"
    protoc {
        artifact = 'com.google.protobuf:protoc:3.11.4'
    }
}
```

This will tell the protobuf Gradle plugin to use the `3.11.4` version of protoc to compile the protos in our project.
You may have noticed above that the Record Layer artifact we are using is `fdb-record-layer-core-pb3` instead of
`fdb-record-layer-core`, which is the record-layer version to use with version 2 of protoc. This also configures
`generatedFileBaseDir` to be a separate directory at the same level as our Java code and proto definitions.

Create `src/main/generated`, `src/main/java`, and `src/main/proto` directories so the structure of our project
source looks like this:
```
src
├── main
│   ├── generated
│   │   └── # generated classes
│   ├── java
│   │   ├── # application code
│   └── proto
│       └── # proto definitions
```

One last step might be necessary to configure your IDE of choice to discover the generated Java source files and to
offer auto-complete suggestions. The additional generated source directory can be added to the list of Java sources
by adding the following to the project's `build.gradle` file:

```groovy
sourceSets {
    main {
        java {
            srcDir 'src/main/generated/main/java'
        }
    }
}
```

Now we are ready to define the record types and indexes we will use through the *record meta-data*.
We can think of this as a type of schema definition for our application. We’ll define our meta-data
as a set of protobuf messages. For this example, we will create a very simple meta-data definition
for our application that will keep track of customer orders for flowers. Add the below protobuf
as the file `record_layer_demo.proto` in the `src/main/proto` directory:

```protobuf
// While we pull in the proto3 org.foundationdb:fdb-record-layer-core-pb3 dependency
// above, we write record-layer protos files using `proto2` syntax.
syntax = "proto2";

option java_outer_classname = "RecordLayerDemoProto";

import "record_metadata_options.proto";

message Order {
    optional int64 order_id = 1;
    optional Flower flower = 2;
    optional int32 price = 3;
}

message Flower {
    optional string type = 1;
    optional Color color = 2;
}

enum Color {
    RED = 1;
    BLUE = 2;
    YELLOW = 3;
    PINK = 4;
}

message UnionDescriptor {
    option (com.apple.foundationdb.record.record).usage = UNION;
    optional Order _Order = 1;
}
```

We have one top level record type `Order`, which has a unique `order_id` and a `price`.
All top level record types need to have a primary key that they will be indexed by in the record store.
While it is possible to define a primary key (and secondary indexes) in our protobuf definition using
the meta-data options, it is generally preferred to define indexes in our application code using a `RecordMetaDataBuilder`.
Each `Order` also has a flower type, which is represented by a nested message containing the name
of the type as well as the color (itself an enum). Later in our application code we will show how we
can query the values of nested fields.

Finally, the Record Layer requires we have a `UnionDescriptor` message which contains all of the top level
record types to be stored in our record store (here only `Order`). We must either set the `usage = UNION`
option for this message or we can omit the option and instead name the message `RecordTypeUnion`.

## Creating an Application

Run `./gradlew generateProto` to see that the above configuration is correct.
You should see the generated code put into `src/main/generated/main/java/RecordLayerDemoProto.java`.

Now we're ready to start developing our application. Create a demo app class `src/main/java/Demo.java` and add
a main method.

```java
public class Demo {
    public static void main(String[] args) {
    }
}
```

We will do all of the development for this example within this class. Code snippets can be assumed to be in the `main` method.
It will be useful to define a couple of helper methods as well. In those cases, the full helper method will be reproduced here.

Start by opening a connection to the FDB database (go ahead and start the FDB server if you haven't already):

```java
FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();
```

The no-argument version of `getDatabase` will use the default cluster file to connect to FDB.
This should be fine assuming you installed FDB according to the instructions at the beginning of the guide.
If you want to use a different cluster file, you can pass a `String` with the path to the cluster file to this method instead.

Next, we need to define a key space for our record store. The Record Layer provides the `KeySpacePath` API
which allows us to build a logical directory structure for organizing our record stores (e.g., sharding data across stores).
For now, we will only create one record store at a fixed path:

```java
// Define the keyspace for our application
KeySpace keySpace = new KeySpace(new KeySpaceDirectory("record-layer-demo", KeySpaceDirectory.KeyType.STRING, "record-layer-demo"));
// Get the path where our record store will be rooted
KeySpacePath path = keySpace.path("record-layer-demo");
```

To build the record meta-data, first create the `RecordMetaDataBuilder` and add the `Order` record type from our proto definition:

```java
RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
        .setRecords(RecordLayerDemoProto.getDescriptor());
```

Set the primary key to `order_id`:

```java
metaDataBuilder.getRecordType("Order")
        .setPrimaryKey(Key.Expressions.field("order_id"));
```

Add a secondary index on `price`:

```java
metaDataBuilder.addIndex("Order", new Index("priceIndex", Key.Expressions.field("price")));
```


The primary and secondary index definitions take a `Key.Expression` as an argument. It is possible to have more
complex index definitions (e.g., compound primary keys or fan-out indexes on repeated fields).
For more advanced examples, see the [Record Layer Overview](Overview.md).

Now we have finished with configuration, build the `RecordMetaData`:

```java
RecordMetaData recordMetaData = metaDataBuilder.build();
```

We can now create an instance of our record store. As you may know, FDB is a transactional key-value store.
In the Record Layer, those transactions are wrapped in an `FDBRecordContext`. The `FDBRecordStore` object only has
the lifetime of a single transaction, so we need to provide an `FDBRecordContext` each time we create one:

```java
Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(recordMetaData)
        .setContext(context)
        .setKeySpacePath(path)
        .createOrOpen();
```

We'll use this helper function to cut down on the repetition of setting the (never changing) meta-data and path when we open a new transaction.

A few helpers to improve the readability of our example:

```java
private enum FlowerType {
    ROSE,
    TULIP,
    LILY,
}

private static RecordLayerDemoProto.Flower buildFlower(FlowerType type, RecordLayerDemoProto.Color color) {
    return RecordLayerDemoProto.Flower.newBuilder()
            .setType(type.name())
            .setColor(color)
            .build();
}
```

Now back to the `main` method, let's save a few records:

```java
db.run(context -> {
    FDBRecordStore recordStore = recordStoreProvider.apply(context);
    
    recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
            .setOrderId(1)
            .setPrice(123)
            .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
            .build());
    recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
            .setOrderId(23)
            .setPrice(34)
            .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.PINK))
            .build());
    recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
            .setOrderId(3)
            .setPrice(55)
            .setFlower(buildFlower(FlowerType.TULIP, RecordLayerDemoProto.Color.YELLOW))
            .build());
    recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
            .setOrderId(100)
            .setPrice(9)
            .setFlower(buildFlower(FlowerType.LILY, RecordLayerDemoProto.Color.RED))
            .build());

    return null;
});
```

What is happening here? The `run` method runs the provided function transactionally against FDB, i.e., it
opens a new transaction and provides it wrapped in an `FDBRecordContext` as an argument to the function.
We can then perform some work and return a result (in this case we are just returning `null`). The `run`
method handles opening the transaction for us and attempting to commit the result. If we get a
retriable error on commit it will automatically retry for us (up to a configurable number of maximum retry attempts).
So in the above snippet, we are opening a transaction, creating an instance of the record store, and saving 4 records.
Assuming the method returns normally, the records will be persisted to FDB.

Now open another transaction and see that we can load the records there (and that we get `null` for records we didn't create):

```java
FDBStoredRecord<Message> storedRecord = db.run(context ->
        // load the record
        recordStoreProvider.apply(context).loadRecord(Tuple.from(1))
);
assert storedRecord != null;

// a record that doesn't exist is null
FDBStoredRecord<Message> shouldNotExist = db.run(context ->
        recordStoreProvider.apply(context).loadRecord(Tuple.from(99999))
);
assert shouldNotExist == null;
```

The `loadRecord` method returns an `FDBStoredRecord`, but we want to reconstruct the original `Order`
as defined in our meta-data. This can be accomplished by getting the proto message from the stored message and rebuilding the order:

```java
RecordLayerDemoProto.Order order = RecordLayerDemoProto.Order.newBuilder()
        .mergeFrom(storedRecord.getRecord())
        .build();
System.out.println(order);
```

This prints:

```
order_id: 1
flower {
  type: "ROSE"
  color: RED
}
price: 123
```

which is exactly the record we stored with `order_id` 1.

We can also perform a basic query on our data. Let's look for all orders of roses that are less than $50.
We can see from the records we saved that the expected result is just the record with `order_id = 23`.
Note here that part of our query is a filter on the value of a field in a nested message (we only want roses).
We can create the query:

```java
RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("Order")
        .setFilter(Query.and(
                Query.field("price").lessThan(50),
                Query.field("flower").matches(Query.field("type").equalsValue(FlowerType.ROSE.name()))))
        .build();
```

Here the `matches` method allows us to match against the value of a subfield `type` of a nested message `flower`.
Now run the query, and collect the result as a list of orders:

```java
List<RecordLayerDemoProto.Order> orders = db.run(context -> {
    FDBRecordStore recordStore = recordStoreProvider.apply(context);

    // this returns an asynchronous cursor over the results of our query
    RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(query);
    // let's return it as a list of records
    return cursor
            .map(queriedRecord -> RecordLayerDemoProto.Order.newBuilder()
                .mergeFrom(queriedRecord.getRecord()).build())
            .asList().join();
});
```

Note that we transformed the cursor of `FDBQueriedRecord`s to a cursor over `Order`s by calling map on the cursor.
The [`RecordCursor` class](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/RecordCursor.html)
supports several such methods.

Finally, print the list of orders that the query returned:

```java
orders.forEach(System.out::println);
```

Which shows exactly what we expect:

```
order_id: 23
flower {
  type: "ROSE"
  color: PINK
}
price: 34
```

## Summary

We've covered creating a very simple demo app from scratch using the FDB Record Layer.
We downloaded and started up an FDB server, created a new Java project, brought in the Record Layer dependency,
created a schema and wrote a simple application that uses the Record Layer. This was only a surface level demonstration
of the schema management, querying and indexing capabilities of the Record Layer. For a more complete guide to what you
can do, refer to the [FDB Record Layer Overview](Overview.md).

