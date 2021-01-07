# FoundationDB Record Layer Lucene Index

The lucene index implementation on the record layer consists of two key components. First, it consists TODO 

* **Structured types** - Records are defined and stored in terms of
  [protobuf](https://developers.google.com/protocol-buffers/) messages.
* **Indexes** - The Record Layer supports a variety of different index
  types including value indexes (the kind provided by most databases),
  rank indexes, and aggregate indexes. Indexes and primary keys can
  be defined either via protobuf options or programmatically.

## Documentation

* [Documentation Home](docs/index.md)
