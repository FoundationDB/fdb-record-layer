# FoundationDB Record Layer Documentation

The **FoundationDB Record Layer** is a Java library that provides a record-oriented data store and a relational database interface built on top of the [FoundationDB](https://www.foundationdb.org) key/value store. Its key features are:

* A **[SQL interface](SQL_Reference.md)** for defining schemas, querying data, and manipulating tables using familiar SQL syntax. Java applications interact with the Record Layer through a standard **[JDBC interface](jdbc/index.rst)**.
* A **record-oriented [Java API](Overview.md)**. This API defines records using [protocol buffers](https://protobuf.dev), supports declarative query execution, and offers fine-grained control over storage layout and index maintenance through extension points such as <a href="api/fdb-record-layer-core/com/apple/foundationdb/record/provider/foundationdb/IndexMaintainer.html">index maintainers</a> and custom query components. It is positioned as a low-level alternative to SQL for advanced use cases; most of its capabilities are expected to migrate to the SQL interface over time.
* Reusable **[schema templates](reference/Databases_Schemas_SchemaTemplates.rst)**, which enable massively multi-tenant architectures where each tenant instantiates their own database based on a shared schema that can [safely evolve over time](SchemaEvolution.md).
* Rich **[indexing capabilities](reference/Indexes.rst)**, which include value indexes, aggregate indexes, rank indexes, and indexes on the commit time of records. Indexes are akin to materialized views and can be updated incrementally.
* An advanced **[type system](reference/sql_types.rst)** that, beyond the standard types found in traditional relational databases, includes user-defined struct types, arrays of primitives or complex types, and fixed-dimension numerical vectors for ML embeddings and similarity search.
* An intelligent **query planner**, which performs automatic index selection and query optimization, and supports complex [correlated subqueries](reference/Subqueries.rst), [joins](reference/Joins.rst), aggregation with `GROUP BY`, and ordering (`ORDER BY`). To limit memory use, query plans avoid in-memory buffers and instead rely heavily on stream-based processing over available indexes.
* Full **ACID transactions**, inherited from FoundationDB along with its strong reliability and performance in a distributed setting. To work within the transaction time and size limits, the query API provides continuations that efficiently page through large result sets.
* A scalable, **stateless architecture**. Scaling up is as simple as launching more instances, since there is no server-side state—all data resides in FoundationDB. With millisecond-level store initialization and query execution, it is well-suited to multi-tenancy across thousands of independent database instances. Each tenant’s state is encapsulated, and resource consumption is tightly constrained and balanced across users, even under a wide variety of workloads.

```{toctree}
:hidden:
Welcome <self>
```

```{toctree}
:maxdepth: 1
:caption: User Guide
:hidden:
Overview
Versioning
Getting started (Java) <GettingStarted>
Getting started (SQL) <SQL_Getting_Started>
SchemaEvolution
Extending
FAQ
ReleaseNotes
```

```{toctree}
:maxdepth: 1
:caption: Reference
:hidden:
SQL_Reference
jdbc/index
api/index
```

```{toctree}
:maxdepth: 1
:caption: Development
:hidden:
Building <Building>
Coding_Best_Practices
architecture/index
Contributing <https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md>
Code of conduct <https://github.com/FoundationDB/fdb-record-layer/blob/main/CODE_OF_CONDUCT.md>
```
