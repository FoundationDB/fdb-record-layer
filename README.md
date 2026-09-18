# FoundationDB Record Layer

[![Maven Central](https://img.shields.io/maven-central/v/org.foundationdb/fdb-record-layer-core?label=Maven%20Central)](https://mvnrepository.com/artifact/org.foundationdb/fdb-record-layer-core)
[![Nightly](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/nightly.yml/badge.svg)](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/nightly.yml)

<img alt="FoundationDB logo" src="docs/sphinx/source/FDB_logo.png?raw=true" width="150">

The **FoundationDB Record Layer** is a Java library that provides a record-oriented data store and a relational database interface built on top of the [FoundationDB](https://www.foundationdb.org) key/value store. Applications access it through a SQL interface over JDBC or a lower-level, record-oriented Java API. Its type system extends the standard relational types with user-defined nested structs, arrays, and fixed-dimension vectors for ML embeddings and similarity search. Reusable schema templates enable massively multi-tenant architectures whose shared schemas can safely evolve over time. An intelligent query planner handles joins, aggregation, grouping, sorting, and correlated subqueries, backing them with rich, incrementally-maintained indexes and favoring stream-based processing over in-memory buffers. Designed for distributed, stateless deployments with millisecond-level store initialization and query execution, it scales to thousands of independent database instances while inheriting the full ACID transactional semantics of FoundationDB.

### Links

* [Documentation](https://foundationdb.github.io/fdb-record-layer/) — A comprehensive overview of the FoundationDB Record Layer.
* [Setup](https://foundationdb.github.io/fdb-record-layer/Versioning.html#maven-dependency) — How to add the Java library as a dependency of your build.
* [Release Notes](https://foundationdb.github.io/fdb-record-layer/ReleaseNotes.html) — A log of user-visible changes across releases.
* [GitHub Issues](https://github.com/FoundationDB/fdb-record-layer/issues) — Our issue tracker for bugs and feature requests.
* [foundationdb.org](https://www.foundationdb.org) — The main landing page of the FoundationDB project.
* [FoundationDB Forums](https://forums.foundationdb.org/) — The home for discussion about the FoundationDB project.
