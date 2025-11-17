<img alt="FoundationDB logo" src="docs/sphinx/source/FDB_logo.png?raw=true" width="400">

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads but also has excellent performance for write-intensive workloads. Users interact with the database using API language binding.

To learn more about FoundationDB, visit [foundationdb.org](https://www.foundationdb.org/)

# FoundationDB Record Layer (FRL)

FRL provides a **relational database with SQL support** built on top of FoundationDB, featuring:

* **SQL Database** - SQL support with JDBC connectivity for defining schemas,
  querying data, and managing tables using familiar SQL syntax. The SQL API is under
  active development with frequent enhancements.
* **Advanced Data Types** - Beyond standard SQL types (STRING, INTEGER, FLOAT, BOOLEAN),
  FRL supports:
  * **Nested Structures** - User-defined struct types that can be nested arbitrarily deep
  * **Arrays** - Collections of primitives or complex types
  * **Vectors** - Fixed-dimension numerical vectors for ML embeddings and similarity search
* **Schema Templates** - Reusable schema definitions that enable multi-tenant architectures
  where each tenant gets their own database instance with a shared, evolvable schema.
* **Intelligent Query Planning** - Automatic index selection and query optimization with
  support for JOINs, aggregations (COUNT, SUM, etc.), GROUP BY, and ORDER BY.
  Queries are efficiently executed using index-backed operations without in-memory sorting.
* **Indexes** - Rich indexing capabilities including value indexes, rank indexes, aggregate
  indexes, and indexes on nested fields. Indexes are materialized views that update incrementally.
* **Scalable Architecture** - Designed for distributed, stateless environments with
  millisecond-level store initialization. Perfect for applications managing thousands
  of discrete database instances.
* **ACID Transactions** - Full transactional semantics inherited from FoundationDB,
  with support for continuations for efficiently paging through large result sets.

## Quick Start with SQL

```java
// Connect via JDBC
String url = "jdbc:embed:/__SYS?schema=CATALOG";
Connection conn = DriverManager.getConnection(url);

// Define a schema template with tables and indexes
conn.createStatement().execute("""
    CREATE SCHEMA TEMPLATE my_template
        CREATE TABLE customers (
            customer_id BIGINT,
            name STRING,
            email STRING,
            PRIMARY KEY(customer_id)
        )
        CREATE INDEX email_idx AS
            SELECT email FROM customers ORDER BY email
    """);

// Create a database and schema
conn.createStatement().execute(
    "CREATE DATABASE /my_app/production");
conn.createStatement().execute(
    "CREATE SCHEMA /my_app/production/main WITH TEMPLATE my_template");

// Insert and query data
PreparedStatement insert = conn.prepareStatement(
    "INSERT INTO customers VALUES (?, ?, ?)");
insert.setLong(1, 1);
insert.setString(2, "Alice");
insert.setString(3, "alice@example.com");
insert.executeUpdate();

ResultSet rs = conn.createStatement().executeQuery(
    "SELECT * FROM customers WHERE email = 'alice@example.com'");
```

## Key Features

### Multi-Tenant Schema Templates

Schema templates enable efficient multi-tenant architectures:

```sql
-- Define the template once
CREATE SCHEMA TEMPLATE user_data_template
    CREATE TABLE documents (id BIGINT, content STRING, PRIMARY KEY(id))
    CREATE INDEX content_idx AS SELECT content FROM documents ORDER BY content;

-- Create separate database instances for each tenant
CREATE DATABASE /tenant/user_1;
CREATE SCHEMA /tenant/user_1/data WITH TEMPLATE user_data_template;

CREATE DATABASE /tenant/user_2;
CREATE SCHEMA /tenant/user_2/data WITH TEMPLATE user_data_template;
```

Each tenant's data is completely isolated with its own database, yet all share
the same schema definition for easy management and evolution.

### Advanced Type System

Define complex, nested data structures:

```sql
-- Define custom struct types
CREATE TYPE AS STRUCT address (
    street STRING,
    city STRING,
    postal_code STRING
)

CREATE TYPE AS STRUCT contact_info (
    email STRING,
    phone STRING,
    mailing_address address
)

-- Use in tables with arrays and nesting
CREATE TABLE users (
    user_id BIGINT,
    name STRING,
    contacts contact_info ARRAY,
    PRIMARY KEY(user_id)
)
```

### Vector Support for ML Applications

Store and query high-dimensional vectors for embeddings and similarity search:

```sql
CREATE TABLE embeddings (
    doc_id BIGINT,
    content STRING,
    embedding_half VECTOR(128, HALF),     -- 16-bit precision
    embedding_float VECTOR(768, FLOAT),   -- 32-bit precision
    embedding_double VECTOR(1024, DOUBLE), -- 64-bit precision
    PRIMARY KEY(doc_id)
)
```

Vectors are inserted via JDBC PreparedStatements and can be efficiently stored
and retrieved using the FoundationDB backend.

### Index-Backed Query Execution

FRL's query planner intelligently selects indexes to execute
queries efficiently:

```sql
-- Queries use indexes automatically
SELECT name FROM customers WHERE email = 'alice@example.com';
-- Uses email_idx if available

-- JOINs using comma-separated FROM clause
SELECT c.name, o.order_id
FROM customers c, orders o
WHERE c.customer_id = o.customer_id;

-- Aggregations backed by indexes
SELECT category, COUNT(*)
FROM products
GROUP BY category;
-- Requires ordered index or primary key on category for streaming aggregate, or a aggregate index for direct retrieval

-- ORDER BY requires index or primary key order
SELECT * FROM customers ORDER BY email;
-- Requires index on email (like email_idx above)
```

**Important**: FRL does not perform in-memory sorting or aggregation. Operations like ORDER BY, GROUP BY,
and aggregates require underlying indexes to provide the required ordering.

## Architecture Notes

FRL is designed for:
* **Horizontal scalability** - Thousands of independent database instances
* **Low latency** - Millisecond-level initialization and query execution
* **Stateless services** - No server-side state; all data in FoundationDB
* **Schema evolution** - Templates can evolve over time (template evolution features
  coming to relational layer; currently available via advanced Record Layer API)

## Advanced: Direct Record Layer API

For applications requiring fine-grained control over storage layout, index
maintenance, or features not yet available in the SQL Relational layer, the Record Layer
provides a low-level Java API using Protocol Buffers.

**Note**: This API is maintained for advanced use cases but is being positioned
as a lower-level alternative to the SQL interface. Features available only
through this API will migrate to the SQL layer over time. Long-term support of this lower-level API is not guaranteed once equivalent features are available at the Relational Layer.

Key Record Layer API features:
* **Protobuf-based schema definition** - Define records using `.proto` files
* **Programmatic index management** - `IndexMaintainer` extension points
* **Custom query components** - Extend the query planner
* **Schema evolution** - `MetaDataEvolutionValidator` for safe schema changes
* **Low-level control** - Direct access to FoundationDB operations

See [Record Layer Documentation](https://foundationdb.github.io/fdb-record-layer/Overview.html) for details.

## Documentation

* **Getting Started** - [SQL Quick Start](https://foundationdb.github.io/fdb-record-layer/GettingStarted.html)
* **SQL Reference** - [SQL Commands and Data Types](https://foundationdb.github.io/fdb-record-layer/SQL_Reference.html)
* **Schema Templates** - [Databases, Schemas, and Templates](https://foundationdb.github.io/fdb-record-layer/reference/Databases_Schemas_SchemaTemplates.html)
* **Advanced: Record Layer API** - [Record Layer Overview](https://foundationdb.github.io/fdb-record-layer/Overview.html)
* [Documentation Home](https://foundationdb.github.io/fdb-record-layer/)
* [Contributing](CONTRIBUTING.md)
* [Code of Conduct](CODE_OF_CONDUCT.md)
* [License](LICENSE)

## Getting Help

* **Documentation Issues**: [Submit Documentation Feedback](https://github.com/FoundationDB/fdb-record-layer/issues)
* **Bugs & Feature Requests**: [GitHub Issues](https://github.com/FoundationDB/fdb-record-layer/issues)
* **Community**: [FoundationDB Community Forums](https://forums.foundationdb.org/)
