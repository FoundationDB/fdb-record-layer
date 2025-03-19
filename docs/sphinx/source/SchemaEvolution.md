# Schema Evolution and Meta-data Maintenance

## General Principles

The same meta-data can be used for multiple record stores. Some of these record stores may not have been accessed for some time and need to catch-up to changes made in the meta-data. This is accomplished by storing the version of the last meta-data used in the header of each record store and checking this version against the current meta-data's version whenever the record store is opened.

In order for this to work, it is crucial that the meta-data's versions are maintained properly, increasing with every change and reflecting what has changed.

The following pieces of the meta-data are versioned:

* The overall version for the whole meta-data, incremented with any change.
* The version at which a record type was first added to the meta-data.
* The version at which an index was first added and the version at which its definition was last changed.
* The version at which a now-removed index was first added and the version at which the removal happened.

Methods on `RecordMetaDataBuilder` maintain these versions. It and the objects it holds, such as record types and indexes, also expose setting their versions explicitly.

So one of the following techniques should be used:

* If building from code and expecting to use the meta-data with existing stores (that is, unless erasing everything as the first step of testing), repeat and undo old operations on the builder instead of deleting the old code.
* If storing a serialized form of the meta-data in an external store like `FDBMetaDataStore`, read the existing meta-data from the store into a builder, make changes, and persist back from the builder.
* If building the meta-data from external source, such as a higher-level schema management system, store versions there or compare when merging with the previous version and increment when changed.

## Validating Changes

If it is possible to know both the previous and new values for the meta-data, one can validate that the new meta-data has been responsibly evolved from the old meta-data by using the [`MetaDataEvolutionValidator`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/metadata/MetaDataEvolutionValidator.html) class. Basic usage is something like:

```java
MetaDataEvolutionValidator validator = MetaDataEvolutionValidator.getDefaultInstance();
validator.validate(oldMetaData, newMetaData);
``` 

The `validate` function will throw a `MetaDataException` if the new meta-data has not been evolved from the old one in a way that does not require migrating data already stored by the record store. It is generally strict about the kinds of meta-data modifications it allows, and it attempts to be conservative. For example, by default, it will not allow modifications to existing indexes that require they be rebuilt as the assumption is that such a modification is likely to be accidental. The validator can be made more lenient by setting various options when it is initialized.

The [`FDBMetaDataStore`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/FDBMetaDataStore.html) will automatically use this class to validate the meta-data when saving a newer version of the meta-data by reading the existing meta-data from the database and comparing it with the meta-data provided. It is encouraged that users with an alternate scheme for storing and managing meta-data do the same to avoid accidentally introducing an incompatible change.

## Specific Operations

### Update to using proto3 syntax from proto2

If your records descriptor is initially written using `proto2` syntax (regardless of whether your Protobuf version is 2 or 3), there is no clear upgrade path to begin using `proto3` syntax without losing data. In particular, because records written using `proto3` syntax cannot distinguish between unset values (which the Record Layer uses to indicate that a field is `null`) and fields set to the default value for that type, if there are any indexes defined on any field, the index entries for anything set to the default value for that field will no longer be accurate. It is therefore advised that any existing record types continue to use `proto2` syntax, though new record types can be added using `proto3`. For more information on how using `proto2` and `proto3` syntax affects nullability semantics, see [Indexing and Querying of missing / null values](Overview.md#indexing-and-querying-of-missing--null-values).

### Changing whether long records are split

By default, the record meta-data assumes that all records can fit in a single FoundationDB key-value pair. If records are expected to exceed the [FoundationDB value size limit](https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values), one should enable the `split_long_records` option on the meta-data. It is safe to enable the splitting of larger records after data has been written as long as the record store was created with a format version of at least [`FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION`](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/FDBRecordStore.html#SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION). This should include all record stores created using Record Layer version 2.1 or newer unless the format version was explicitly set to some lower value.

### Changing whether record commit versions are stored

It is safe to enable or disable the `store_record_versions` schema option on the meta-data. If there are any ["version" indexes](https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/com/apple/foundationdb/record/provider/foundationdb/indexes/VersionIndexMaintainer.html) defined on the record store when the option is disabled, those indexes must also be removed from the meta-data.

### Add a field to an existing record type

Because of how Protobuf works, all existing records will have the new field but uninitialized. So this is a relatively safe operation. Note however that by default, the Record Layer will log a warning if it deserializes a Protobuf message with unknown fields, and application code that marshals Protobuf messages to their own types can easily drop these fields accidentally. Therefore, some care must be taken to make sure that before any client begins writing to that field that all clients have been updated to use the new meta-data.

### Remove a field from a record type

Rather than actually removing, it is better if the field is marked as deprecated and ignored by the application. If the field is removed, the Record Layer will print a warning when a record with that field is loaded, "Deserialized message has unknown fields."
Under no circumstances should a new field be added with an existing field number, because old records with the old field will appear to have the new one with the same value (or a garbled version of it if the type changed).

### Rename a field in a record type

Serialization is based on field numbers, not field names. So, for the record itself, this is invisible.
However, index and primary key definitions in the meta-data itself are based on field names, so these will also need to be updated in exactly the same way. At present, to avoid this appearing to be a change requiring an index rebuild, the index's subspace key and version need to be copied from the old definition.

### Change the type of a field in a Record Type

For fields that are not part of any index or a record type's primary key, any type change that is valid according to the Protobuf guidelines on [updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating) is acceptable. However, for indexed fields, there are additional restrictions. The only safe type change that does not require an index rebuild is to update a 32-bit, variable length integer to its 64-bit version. (That is, `int32` to `int64` and `sint32` to `sint64`.) In particular, note that fields of type `string` and `bytes` have incompatible serialization formats within indexes and primary keys, as do fields of type `bool` and integral types. Likewise, it is safe to change an `optional` field into a `repeated` field only if there are no indexes on that field (or, equivalently, all existing indexes on that field are dropped or rebuilt).

### Add a new record type

This is always safe. No instances of the new record type can exist in the record store.
The new record type should be given a new field in the union message type. It is also good practice to set the `since_version` field associated with that record type to the meta-data version in which it is added. This allows index builds on any indexes defined on the new type to be skipped when record stores using the meta-data are upgraded.

### Remove a record type

Instead of removing a record type, mark it and its field in the union message as deprecated.
When scanning all records, it is then still possible to encounter instances of the old record type, which should be ignored.

### Rename a record type

Serialization is based on the field number in the union message, so for the record itself, this is invisible.
However, index definitions in the meta-data are based on record type names, so these will also need to be updated in exactly the same way.

### Add an index to a record type

When a new index is added, it must be populated for existing records of the type(s) to which it applies before it can be used by queries. To accomplish this, the index objects in the meta-data are versioned for when they were added / removed / changed and the record store is versioned for when it was last accessed. Additionally, the record store records the state of every index in the meta-data as one of:

* **readable**. The normal state of an index; record saves update the index and queries are free to consider the index when planning.
* **write-only**. Saves still update the index, but it is not yet considered for queries.
* **disabled**. The index is ignored for this record store.

When a record store is opened and there are indexes with newer versions than the last time the record store was accessed, those indexes need to be built if they are to be used for queries. If there are only a few records — by default less than 200 — in the record store, the transaction that is opening the record store will scan them to populate the index and it becomes readable immediately. The reason for this check is that scanning more than a few records might unacceptably delay an unrelated request and might also run up against the five-second transaction time limit.

If there are too many records in the record store the index is marked write-only instead. The client can use the `OnlineIndexer` to build the index in as many separate transactions as needed. While the online index build is running, the record store is still usable for normal operations, just without the new index. Also, since the index is write-only, changes to the record store will themselves keep it up to date, avoiding a situation where the online indexer can never catch up.

It is important to understand that the scan to build an index is a direct scan of the records in the record store, which means that it must skip over records of types to which the index does not apply. Therefore, for purposes of determining “a few records,” the count of all records in the record store is used, and not just the count of records of the type(s) being indexed. The exception to this is when the indexed record type has the record-type key prefix that segregates it from other records of different types. In that case, both the counting and the scanning can be limited to just those records, since they are contiguous in the database.

### Remove an index

If an index has been removed since the last time the record store's meta-data has been updated, all of the data for the index are cleared. This is a cheap operation because the index is contiguous in the database. If an index has been added and removed between the last time the meta-data for a store was updated and now, then nothing needs to be done.

### Change an Index Definition

As an alternative to removing an old index and adding a new one, the definition of an index can be changed while keeping the same location in the database. In this case, the index will be marked write-only and the user should clear out the index prior to beginning their rebuilds so that only the new definition is used.

Care must be taken, though, when the old and new definitions are alternatives, with the new one being perhaps more efficient for some queries. In this case, it is possible for the index to get marked write-only while it needs to get built by the online indexer, which means that neither the old nor the new definition will be available for queries. If the client needs one of the indexes, then it must add the new one and only remove the old one after the new one is built.

### Add a new record type and indexes at the same time

In this case, no instances of the new record type can exist in the data store, so the new index can be marked readable right away no matter how many other records of different types there are.


