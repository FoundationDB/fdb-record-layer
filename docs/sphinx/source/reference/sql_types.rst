==============
SQL Data Types
==============

.. _sql-types:


Primitive Types
###############

As with most relational databases, the FDB Relational Layer supports the expected types--referred to as "primitive types" in Relational Layer parlance:

* Strings (STRING)
* Scalars (INTEGER/BIGINT)
* Floating Point (FLOAT/DOUBLE)
* Booleans (BOOLEAN)
* byte arrays (BYTES)

In addition, the Relational Layer supports two distinct user-definable types that are core to how it operates: `struct types`_ and `array types`_.


.. _struct_types:

Struct Types
############

You can define a *struct type* (often interchangeably referred to as a *nested type*). A struct is a tuple of columns that allow the same types as a table does, but does _not_ have a primary key. Struct types are "nested" within another owning type, and are stored in the same location as their owning record. For example, a table :sql:`foo` can have the following layout (using the :doc:`DDL <sql_commands/DDL>` syntax):


.. code-block:: sql

    CREATE TYPE AS STRUCT nested_type (d INT64, e STRING)
    CREATE TABLE foo (a STRING, b DOUBLE, c nested_type, PRIMARY KEY(a));

In this example, :sql:`nested_type` is a struct within the table `foo`, and its full contents are materialized alongside the full record for each entry in the `foo` table.

Struct types can have columns which are themselves struct types. Thus, this example is fully allowed:

.. code-block:: sql

    CREATE TYPE AS STRUCT nested_nested_type (f STRING, g STRING)
    CREATE TYPE AS STRUCT nested_type (d INT64, e STRING, f nested_nested_type)
    CREATE TABLE foo (a STRING, b DOUBLE, c nested_type, PRIMARY KEY(a));

In this example, :sql:`nested_type` is a struct within the table :sql:`foo`, and :sql:`nested_nested_type` is a struct within the type :sql:`nested_type`.

The Relational Layer makes no direct limitations on how many structs can be nested within a single type, nor does it limit how deeply nested struct types can be. It is probably worth noting that there are practical limits, like the JVM stack size, that should discourage an adopter from designing types which are nested thousands deep. The general expectation is that nesting depth is probably within the 10s, not 1000s. Note that such "soft" limits may be restructured into hard limits (such as throwing an error if structs are nested more than X level deep) in the future.

.. _array_types:

Array types
###########

The Relational DDL also supports the definition of *array types*. An array is a (finite) collection of records which share the same layout, and which are contained by an owning type. The elements of an array have their own layout, and can be one or more of any accepted column types.

Thus, an array could be of a single primitive type:

.. code-block:: sql

    CREATE TABLE foo (a STRING, b STRING ARRAY);

In this example, `b` is an array with a single :sql:`STRING` column.

Arrays can also be created with struct columns:

.. code-block:: sql

    CREATE TYPE AS STRUCT nested_struct (b STRING, d STRING)
    CREATE TABLE structArray (a STRING, c nested_struct array);

In this example, `c` is an array, and each record within the array is a struct of type :sql:`nested_struct`. You can generally treat an array as a "nested `ResultSet`"--that is to say, you can just pull up a `ResultSet` of an array type, and interrogate it as if it were the output of its own query.

It is possible to nest arrays within structs, and structs within arrays, to an arbitrary depth (limited by the JVM's stack size, currently).

.. _vector_types:

Vector Types
############

The Relational Layer supports *vector types* for storing fixed-size numerical vectors, commonly used in machine learning and similarity search applications. A vector type represents a fixed-dimensional array of floating-point numbers with a specific precision.

Vector Type Declaration
=======================

Vectors are declared using the :sql:`VECTOR(dimension, precision)` syntax, where:

* **dimension**: The number of elements in the vector (must be a positive integer)
* **precision**: The floating-point precision, which can be:

  * :sql:`HALF` - 16-bit half-precision floating-point (2 bytes per element)
  * :sql:`FLOAT` - 32-bit single-precision floating-point (4 bytes per element)
  * :sql:`DOUBLE` - 64-bit double-precision floating-point (8 bytes per element)

Examples of vector column definitions:

.. code-block:: sql

    CREATE TABLE embeddings (
        id BIGINT,
        embedding_half VECTOR(128, HALF),
        embedding_float VECTOR(128, FLOAT),
        embedding_double VECTOR(128, DOUBLE),
        PRIMARY KEY(id)
    );

Vectors can also be used within struct types:

.. code-block:: sql

    CREATE TYPE AS STRUCT model_embedding (
        model_name STRING,
        embedding VECTOR(512, FLOAT)
    )
    CREATE TABLE documents (
        id BIGINT,
        content STRING,
        embedding model_embedding,
        PRIMARY KEY(id)
    );

Internal Storage Format
=======================

Vectors are stored as byte arrays with the following format:

* **Byte 0**: Vector type identifier (0 = HALF, 1 = FLOAT, 2 = DOUBLE)
* **Remaining bytes**: Vector components in big-endian byte order

The storage size for each vector is:

* :sql:`VECTOR(N, HALF)`: 1 + (2 × N) bytes
* :sql:`VECTOR(N, FLOAT)`: 1 + (4 × N) bytes
* :sql:`VECTOR(N, DOUBLE)`: 1 + (8 × N) bytes

Working with Vectors
====================

Vector Literals and Prepared Statements
----------------------------------------

**Important**: Vector literals are not directly supported in SQL. Vectors must be inserted using **prepared statement
parameters** through the JDBC API.

In the JDBC API, you would create a prepared statement and bind vector parameters using the appropriate Java objects
(e.g., :java:`HalfRealVector`, :java:`FloatRealVector`, or :java:`DoubleRealVector`):

.. code-block:: java

    // Java JDBC example
    PreparedStatement stmt = connection.prepareStatement(
        "INSERT INTO embeddings VALUES (?, ?)");
    stmt.setLong(1, 1);
    stmt.setObject(2, new FloatRealVector(new float[]{0.5f, 1.2f, -0.8f}));
    stmt.executeUpdate();

For documentation purposes, the examples below demonstrate vector usage. While vectors can be constructed in SQL by
casting numeric arrays to vector types, **note that inserting vectors requires using prepared statement parameters
through the JDBC API** (as shown in the Java example above). CAST expressions work well for SELECT queries but have
limitations with INSERT statements in prepared statement contexts:

.. code-block:: sql

    -- Example: Constructing vectors using CAST (works in SELECT contexts)
    SELECT CAST([0.5, 1.2, -0.8] AS VECTOR(3, HALF)) AS half_vector;
    SELECT CAST([0.5, 1.2, -0.8] AS VECTOR(3, FLOAT)) AS float_vector;
    SELECT CAST([0.5, 1.2, -0.8] AS VECTOR(3, DOUBLE)) AS double_vector;

Casting Arrays to Vectors
--------------------------

While vector literals are not supported, you can use :sql:`CAST` to convert array expressions to vectors. The source array elements can be of any numeric type (:sql:`INTEGER`, :sql:`BIGINT`, :sql:`FLOAT`, :sql:`DOUBLE`):

.. code-block:: sql

    -- Cast FLOAT array to FLOAT vector
    SELECT CAST([1.2, 3.4, 5.6] AS VECTOR(3, FLOAT)) AS vec;

    -- Cast INTEGER array to HALF vector
    SELECT CAST([1, 2, 3] AS VECTOR(3, HALF)) AS vec;

    -- Cast mixed numeric types to DOUBLE vector
    SELECT CAST([1, 2.5, 3L] AS VECTOR(3, DOUBLE)) AS vec;

The array must have exactly the same number of elements as the vector's declared dimension, or the cast will fail with error code :sql:`22F3H`. Only numeric arrays can be cast to vectors.

Querying Vectors
----------------

Vectors can be selected and compared like other column types. When comparing vectors in WHERE clauses, you would typically use prepared statement parameters in your Java/JDBC code, but for illustration purposes, vectors can also be constructed using CAST:

.. code-block:: sql

    -- Select vectors
    SELECT embedding FROM embeddings WHERE id = 1;

    -- Compare vectors for equality (in actual code use PreparedStatement for better performance)
    SELECT id FROM embeddings WHERE embedding = CAST([0.5, 1.2, -0.8] AS VECTOR(3, FLOAT));

    -- Compare vectors for inequality
    SELECT id FROM embeddings WHERE embedding != CAST([1.0, 2.0, 3.0] AS VECTOR(3, FLOAT));

    -- Check for NULL vectors
    SELECT id FROM embeddings WHERE embedding IS NULL;
    SELECT id FROM embeddings WHERE embedding IS NOT NULL;

    -- Use IS DISTINCT FROM for NULL-safe comparisons
    SELECT embedding IS DISTINCT FROM CAST([0.5, 1.2, -0.8] AS VECTOR(3, FLOAT)) FROM embeddings;

Vectors in Struct Fields
-------------------------

When vectors are nested within struct types, you can access them using dot notation. As with all vector comparisons, you would use prepared statement parameters in your JDBC code:

.. code-block:: sql

    -- Access vector within a struct
    SELECT embedding.embedding FROM documents WHERE id = 1;

    -- Filter by vector within struct (in actual code use PreparedStatement for better performance)
    SELECT id FROM documents
    WHERE embedding.embedding = CAST([0.5, 1.2, -0.8] AS VECTOR(3, FLOAT));

    -- Check NULL for vector field in struct
    SELECT id FROM documents WHERE embedding.embedding IS NULL;

Supported Operations
====================

The following operations are supported on vector types:

* **Equality comparison** (:sql:`=`, :sql:`!=`)
* **NULL checks** (:sql:`IS NULL`, :sql:`IS NOT NULL`)
* **NULL-safe comparison** (:sql:`IS DISTINCT FROM`, :sql:`IS NOT DISTINCT FROM`)
* **CAST from numeric arrays** to vectors

Note that mathematical operations (addition, subtraction, dot product, etc.) are performed through the Java API using the :java:`RealVector` interface and its implementations (:java:`HalfRealVector`, :java:`FloatRealVector`, :java:`DoubleRealVector`), not through SQL.


