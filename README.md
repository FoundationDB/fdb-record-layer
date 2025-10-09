<img alt="FoundationDB logo" src="docs/sphinx/source/FDB_logo.png?raw=true" width="400">

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads but also has excellent performance for write-intensive workloads. Users interact with the database using API language binding.

To learn more about FoundationDB, visit [foundationdb.org](https://www.foundationdb.org/)

# FoundationDB Record Layer

The Record Layer is a Java API providing a record-oriented store on top of FoundationDB, 
(very) roughly equivalent to a simple relational database, featuring:

* **Structured types** - Records are defined and stored in terms of
  [protobuf](https://developers.google.com/protocol-buffers/) messages.
* **Indexes** - The Record Layer supports a variety of different index
  types including value indexes (the kind provided by most databases),
  rank indexes, and aggregate indexes. Indexes and primary keys can
  be defined either via protobuf options or programmatically.
* **Complex types** - Support for complex types, such as lists and
  nested records, including the ability to define indexes against
  such nested structures.
* **Queries** - The Record Layer does not provide a query language, however
  it provides query APIs with the ability to scan, filter, and sort
  across one or more record types, and a query planner capable of
  automatic selection of indexes.
* **Many record stores, shared schema** - The Record Layer provides the
  ability to support many discrete record store instances, all with
  a shared (and evolving) schema. For example, rather than modeling a
  single database in which to store all users' data, each user can be
  given their own record store, perhaps sharded across different FDB
  cluster instances.
* **Very light weight** - The Record layer is designed to be used in a
  large, distributed, stateless environment. The time between opening
  a store and the first query is intended to be measured in milliseconds.
* **Extensible** - New index types and custom index key expressions
  may be dynamically incorporated into a record store.

The Record Layer may be used directly or provides an excellent foundational
layer on which more complex systems can be constructed.

## Documentation

* [Documentation Home](https://foundationdb.github.io/fdb-record-layer/)
* [Contributing](CONTRIBUTING.md)
* [Code of Conduct](CODE_OF_CONDUCT.md)
* [License](LICENSE)


just force a test run
