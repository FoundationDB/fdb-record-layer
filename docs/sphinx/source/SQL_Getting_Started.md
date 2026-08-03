# Getting started with the SQL layer

## Quick start

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

## Key features

### Multi-tenant schema templates

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

### Advanced type system

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

### Vector support for ML applications

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

### Index-backed query execution

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

