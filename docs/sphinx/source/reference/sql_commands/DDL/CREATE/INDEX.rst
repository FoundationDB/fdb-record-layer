============
CREATE INDEX
============

Clause in a :ref:`schema template definition <create-schema-template>` to create an index. The Record Layer supports
two different syntaxes for creating indexes:

1. **INDEX AS SELECT** - A query-based syntax inspired by materialized views
2. **INDEX ON** - A traditional syntax that creates indexes on views or tables

Both syntaxes create indexes that are maintained incrementally. For any given record insert, update, or delete,
the system constructs the difference that needs to be applied to each index in order to update it without needing to
completely rebuild it. This means there are limitations to what kinds of indexes can be created. See
:ref:`index_definition` for more details.

Syntax
======

.. raw:: html
    :file: INDEX.diagram.svg

INDEX AS SELECT Syntax
======================

The ``INDEX AS SELECT`` syntax uses a query to define the index structure. This syntax is inspired by materialized
views and allows you to define indexes using familiar SQL SELECT queries with GROUP BY for aggregate indexes.

Basic Form
----------

.. code-block:: sql

    CREATE INDEX indexName AS query

The ``query`` must be a SELECT statement that returns the columns to be indexed. The index structure is derived
from the query's SELECT list and ORDER BY clause.

Examples
--------

**Simple Value Index**

.. code-block:: sql

    CREATE INDEX idx_price AS
        SELECT price
        FROM products
        ORDER BY price

**Covering Value Index**

.. code-block:: sql

    CREATE INDEX idx_category_price AS
        SELECT category, price, name
        FROM products
        ORDER BY category, price

This creates an index with ``category`` and ``price`` in the key, and ``name`` as a covered column.

**Aggregate Index (SUM)**

.. code-block:: sql

    CREATE INDEX idx_sales_by_category AS
        SELECT category, SUM(amount)
        FROM sales
        GROUP BY category

**Aggregate Index (COUNT)**

COUNT(*) counts all records in each group:

.. code-block:: sql

    CREATE INDEX idx_count_by_region AS
        SELECT region, COUNT(*)
        FROM sales
        GROUP BY region

COUNT(column) counts non-NULL values:

.. code-block:: sql

    CREATE INDEX idx_count_non_null AS
        SELECT category, COUNT(quantity)
        FROM sales
        GROUP BY category

**Aggregate Index (MIN/MAX)**

MIN and MAX support permuted ordering, where the aggregate value can appear at different positions in the key:

.. code-block:: sql

    CREATE INDEX idx_max_by_category AS
        SELECT category, MAX(amount)
        FROM sales
        GROUP BY category
        ORDER BY category, MAX(amount)

The aggregate can also appear in the middle of the key:

.. code-block:: sql

    CREATE INDEX idx_cat_max_region AS
        SELECT category, MAX(amount), region
        FROM sales
        GROUP BY category, region
        ORDER BY category, MAX(amount), region

**Aggregate Index (MIN_EVER/MAX_EVER)**

MIN_EVER and MAX_EVER track the minimum/maximum value ever seen, even after deletions. These are useful for
maintaining historical extrema:

.. code-block:: sql

    CREATE INDEX idx_min_ever_by_category AS
        SELECT category, MIN_EVER(price)
        FROM products
        GROUP BY category

    CREATE INDEX idx_max_ever_by_region AS
        SELECT region, MAX_EVER(sales_amount)
        FROM transactions
        GROUP BY region

For legacy compatibility with older versions, you can use the LEGACY_EXTREMUM_EVER attribute:

.. code-block:: sql

    CREATE INDEX idx_min_ever_legacy AS
        SELECT category, MIN_EVER(price)
        FROM products
        GROUP BY category
        WITH ATTRIBUTES LEGACY_EXTREMUM_EVER

**Bitmap Aggregate Index**

Bitmap indexes use specialized functions for efficient set operations and are particularly useful for filtering
on high-cardinality columns:

.. code-block:: sql

    CREATE INDEX idx_bitmap_by_category AS
        SELECT bitmap_construct_agg(bitmap_bit_position(id)) AS bitmap,
               category,
               bitmap_bucket_offset(id) AS offset
        FROM products
        GROUP BY category, bitmap_bucket_offset(id)

For ungrouped bitmap indexes:

.. code-block:: sql

    CREATE INDEX idx_bitmap_all AS
        SELECT bitmap_construct_agg(bitmap_bit_position(id)) AS bitmap,
               bitmap_bucket_offset(id) AS offset
        FROM products
        GROUP BY bitmap_bucket_offset(id)

**Aggregate with Expressions in GROUP BY**

You can use expressions in GROUP BY clauses:

.. code-block:: sql

    CREATE INDEX idx_sum_by_expression AS
        SELECT amount + 100, MAX(quantity)
        FROM sales
        GROUP BY amount + 100

    CREATE INDEX idx_multi_expression AS
        SELECT category_id + region_id, status + 10, MIN(price)
        FROM products
        GROUP BY category_id + region_id, status + 10

**Filtered Index**

.. code-block:: sql

    CREATE INDEX idx_expensive_products AS
        SELECT name, price
        FROM products
        WHERE price > 100
        ORDER BY price

**Multiple Grouping Columns**

.. code-block:: sql

    CREATE INDEX idx_sales_by_category_region AS
        SELECT category, region, SUM(amount)
        FROM sales
        GROUP BY category, region

**Descending Order**

.. code-block:: sql

    CREATE INDEX idx_price_desc AS
        SELECT price
        FROM products
        ORDER BY price DESC

**Mixed Ordering**

.. code-block:: sql

    CREATE INDEX idx_category_desc_price_asc AS
        SELECT category, price
        FROM products
        ORDER BY category DESC, price ASC

**NULL Ordering**

.. code-block:: sql

    CREATE INDEX idx_rating_nulls_last AS
        SELECT rating
        FROM products
        ORDER BY rating ASC NULLS LAST

    CREATE INDEX idx_supplier_desc_nulls_first AS
        SELECT supplier
        FROM products
        ORDER BY supplier DESC NULLS FIRST

Aggregate Index Capabilities
-----------------------------

Aggregate indexes support the following aggregate functions:

**Supported Aggregate Functions**

- ``SUM(column)`` - Sum of values
- ``COUNT(*)`` - Count of all rows
- ``COUNT(column)`` - Count of non-NULL values
- ``MIN(column)`` - Minimum value (supports permuted ordering)
- ``MAX(column)`` - Maximum value (supports permuted ordering)
- ``MIN_EVER(column)`` - Historical minimum (persists across deletions)
- ``MAX_EVER(column)`` - Historical maximum (persists across deletions)
- ``BITMAP_CONSTRUCT_AGG(bitmap_bit_position(column))`` - Bitmap construction for set operations

**Permuted Ordering**

MIN and MAX indexes support permuted ordering, meaning the aggregate value can appear at any position in the
ORDER BY clause:

.. code-block:: sql

    -- Aggregate at the end (standard)
    CREATE INDEX idx1 AS
        SELECT category, MAX(amount)
        FROM sales
        GROUP BY category
        ORDER BY category, MAX(amount)

    -- Aggregate in the middle
    CREATE INDEX idx2 AS
        SELECT category, MAX(amount), region
        FROM sales
        GROUP BY category, region
        ORDER BY category, MAX(amount), region

    -- Aggregate at the beginning
    CREATE INDEX idx3 AS
        SELECT MIN(amount), category, region
        FROM sales
        GROUP BY category, region
        ORDER BY MIN(amount), category, region

**Aggregate Index Limitations**

- Only one aggregate function per index
- Aggregate columns must be integer types (bigint) for most functions (SUM, COUNT, MIN, MAX)
- MIN_EVER and MAX_EVER work with strings and other comparable types
- Expressions in GROUP BY are supported
- WHERE clauses can be used with aggregate indexes for filtered aggregates

INDEX ON Syntax
===============

The ``INDEX ON`` syntax creates an index on an existing view or table using a traditional columnar specification.
This syntax is particularly useful when combined with views that define filtering or aggregation logic.

Basic Form
----------

.. code-block:: sql

    CREATE INDEX indexName ON source(columns) [INCLUDE(valueColumns)] [OPTIONS(...)]

Where:
- ``source`` is a table or view name
- ``columns`` specifies the index key columns (with optional ordering)
- ``INCLUDE`` clause adds covered columns stored as values
- ``OPTIONS`` clause specifies index-specific options

Examples
--------

**Simple Value Index**

.. code-block:: sql

    CREATE INDEX idx_price ON products(price)

**Multi-Column Index**

.. code-block:: sql

    CREATE INDEX idx_category_price ON products(category, price)

**Covering Index with INCLUDE**

.. code-block:: sql

    CREATE INDEX idx_category_price_covering ON products(category, price)
        INCLUDE(name, stock)

This creates an index with ``category`` and ``price`` in the key, and ``name`` and ``stock`` as covered values.

**Aggregate Index (SUM)**

First define a view with the aggregation:

.. code-block:: sql

    CREATE VIEW v_sales_by_category AS
        SELECT category, SUM(amount) AS total_amount
        FROM sales
        GROUP BY category

    CREATE INDEX idx_sales_by_category ON v_sales_by_category(category)
        INCLUDE(total_amount)

**Aggregate Index (COUNT)**

.. code-block:: sql

    CREATE VIEW v_count_by_region AS
        SELECT region, COUNT(*) AS record_count
        FROM sales
        GROUP BY region

    CREATE INDEX idx_count_by_region ON v_count_by_region(region)
        INCLUDE(record_count)

**Filtered Index**

First define a view with the filter:

.. code-block:: sql

    CREATE VIEW v_expensive_products AS
        SELECT name, price
        FROM products
        WHERE price > 100

    CREATE INDEX idx_expensive_products ON v_expensive_products(price)

**Custom Ordering**

.. code-block:: sql

    CREATE INDEX idx_category_desc ON products(category DESC, price ASC)

**NULL Ordering**

.. code-block:: sql

    CREATE INDEX idx_rating_nulls_last ON products(rating ASC NULLS LAST)

Column Ordering and NULL Handling
----------------------------------

When creating an index using the ``INDEX ON`` syntax, each key column can specify sorting criteria and null semantics
to control how values are ordered in the index.

**Sorting Criteria**

Each key column supports the following sort orders:

- ``ASC`` (ascending) - Values are sorted from smallest to largest (default if not specified)
- ``DESC`` (descending) - Values are sorted from largest to smallest

**NULL Semantics**

You can control where NULL values appear in the sort order:

- ``NULLS FIRST`` - NULL values appear before non-NULL values
- ``NULLS LAST`` - NULL values appear after non-NULL values

Default NULL behavior:
- For ``ASC`` ordering: ``NULLS FIRST`` is the default
- For ``DESC`` ordering: ``NULLS LAST`` is the default

**Syntax Options**

The ordering clause for each column can take the following forms:

1. Sort order only: ``columnName ASC`` or ``columnName DESC``
2. Sort order with null semantics: ``columnName ASC NULLS LAST`` or ``columnName DESC NULLS FIRST``
3. Null semantics only: ``columnName NULLS FIRST`` or ``columnName NULLS LAST`` (uses default ASC ordering)

**Examples**

.. code-block:: sql

    -- Ascending order with nulls last
    CREATE INDEX idx_price ON products(price ASC NULLS LAST)

    -- Descending order with nulls first
    CREATE INDEX idx_rating ON products(rating DESC NULLS FIRST)

    -- Specify only null semantics (ascending is implicit)
    CREATE INDEX idx_stock ON products(stock NULLS LAST)

    -- Mixed ordering across multiple columns
    CREATE INDEX idx_complex ON products(
        category ASC NULLS FIRST,
        price DESC NULLS LAST,
        name ASC
    )

VECTOR INDEX Syntax
===================

The ``VECTOR INDEX`` syntax creates an index specifically designed for vector similarity search using the HNSW
(Hierarchical Navigable Small World) algorithm. Vector indexes enable efficient approximate nearest neighbor (ANN)
search on high-dimensional vector data.

Basic Form
----------

.. code-block:: sql

    CREATE VECTOR INDEX indexName USING HNSW ON source(vectorColumn)
        [PARTITION BY(partitionColumns)]
        [OPTIONS(...)]

Where:
- ``source`` is a table or view name
- ``vectorColumn`` is a column of type ``vector(dimensions, float)``
- ``PARTITION BY`` clause specifies partitioning columns (optional but recommended)
- ``OPTIONS`` clause specifies HNSW-specific configuration options

.. note::
   The ``PARTITION BY`` clause is only applicable to vector indexes. It is not supported for regular value indexes
   created with the ``INDEX ON`` syntax. Partitioning helps organize vectors by category or tenant, improving
   query performance for vector similarity searches pertaining specific category or tenant.

Examples
--------

**Simple Vector Index**

.. code-block:: sql

    CREATE TABLE products(
        id bigint,
        name string,
        embedding vector(128, float),
        primary key(id)
    )

    CREATE VECTOR INDEX idx_embedding USING HNSW ON products(embedding)

**Vector Index with Partitioning**

Partitioning is recommended for better performance and to organize vectors by category or tenant:

.. code-block:: sql

    CREATE TABLE products(
        id bigint,
        category string,
        embedding vector(256, float),
        primary key(id)
    )

    CREATE VECTOR INDEX idx_embedding USING HNSW ON products(embedding)
        PARTITION BY(category)

**Vector Index with Custom Options**

.. code-block:: sql

    CREATE VECTOR INDEX idx_embedding USING HNSW ON products(embedding)
        PARTITION BY(category)
        OPTIONS (
            CONNECTIVITY = 16,
            M_MAX = 32,
            EF_CONSTRUCTION = 200,
            METRIC = COSINE_METRIC
        )

**Vector Index on Filtered View**

.. code-block:: sql

    CREATE VIEW v_active_products AS
        SELECT id, embedding, category
        FROM products
        WHERE status = 'active'

    CREATE VECTOR INDEX idx_active_embeddings USING HNSW ON v_active_products(embedding)
        PARTITION BY(category)

**Vector Index with RabitQ Quantization**

RabitQ is a quantization technique that reduces memory usage for high-dimensional vectors:

.. code-block:: sql

    CREATE VECTOR INDEX idx_embedding USING HNSW ON products(embedding)
        PARTITION BY(category)
        OPTIONS (
            USE_RABITQ = true,
            RABITQ_NUM_EX_BITS = 4,
            MAINTAIN_STATS_PROBABILITY = 0.01
        )

**Vector Index with Statistics Sampling**

.. code-block:: sql

    CREATE VECTOR INDEX idx_embedding USING HNSW ON products(embedding)
        PARTITION BY(category)
        OPTIONS (
            SAMPLE_VECTOR_STATS_PROBABILITY = 0.05
        )

Vector Index Options
--------------------

HNSW Algorithm Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~

``CONNECTIVITY`` (or ``M``)
    The number of bi-directional links created for each node during construction. Higher values improve recall
    but increase memory usage and construction time. Default: 16.

``M_MAX``
    Maximum number of connections per layer. Default: derived from CONNECTIVITY.

``EF_CONSTRUCTION``
    The size of the dynamic candidate list during index construction. Higher values improve index quality
    but increase construction time. Default: 200.

Distance Metrics
~~~~~~~~~~~~~~~~

``METRIC``
    The distance metric used for similarity search. Available options:

    - ``EUCLIDEAN_METRIC`` - L2 distance (default)
    - ``MANHATTAN_METRIC`` - L1 distance
    - ``DOT_PRODUCT_METRIC`` - Dot product (for normalized vectors)
    - ``EUCLIDEAN_SQUARE_METRIC`` - Squared L2 distance
    - ``COSINE_METRIC`` - Cosine similarity (recommended for embeddings)

RabitQ Quantization Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``USE_RABITQ``
    Enable RabitQ quantization to reduce memory usage. Default: false.

``RABITQ_NUM_EX_BITS``
    Number of extra bits for RabitQ quantization. Higher values improve accuracy but increase memory usage.
    Valid range: 0-8. Default: 4.

Statistics and Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~

``MAINTAIN_STATS_PROBABILITY``
    Probability of maintaining statistics during updates when using RabitQ. Default: 0.01.

``SAMPLE_VECTOR_STATS_PROBABILITY``
    Probability of sampling vectors for statistics collection. Default: 0.0 (disabled).

Vector Index Limitations
-------------------------

- Only one vector column can be indexed per vector index
- The indexed column must be of type ``vector(dimensions, float)``
- Vector dimensions must be specified at table creation time
- INCLUDE clause is not supported for vector indexes
- Partitioning improves performance but is optional

Comparing Both Syntaxes
========================

The two syntaxes are functionally equivalent and produce identical index structures. The choice between them
is primarily a matter of style and organizational preference.

Value Index Comparison
-----------------------

These two approaches create identical indexes:

**INDEX AS SELECT:**

.. code-block:: sql

    CREATE INDEX idx_category_price AS
        SELECT category, price, name
        FROM products
        ORDER BY category, price

**INDEX ON:**

.. code-block:: sql

    CREATE INDEX idx_category_price ON products(category, price)
        INCLUDE(name)

Aggregate Index Comparison
---------------------------

These two approaches create identical aggregate indexes:

**INDEX AS SELECT:**

.. code-block:: sql

    CREATE INDEX idx_sales_by_category AS
        SELECT category, SUM(amount)
        FROM sales
        GROUP BY category

**INDEX ON:**

.. code-block:: sql

    CREATE VIEW v_sales_by_category AS
        SELECT category, SUM(amount) AS total_amount
        FROM sales
        GROUP BY category

    CREATE INDEX idx_sales_by_category ON v_sales_by_category(category)
        INCLUDE(total_amount)

Parameters
==========

Common Parameters
-----------------

``indexName``
    The name of the index. Must be unique within the schema template.

``UNIQUE`` (optional)
    Specifies that the index should enforce uniqueness constraints.

INDEX AS SELECT Parameters
---------------------------

``query``
    A SELECT statement that defines the index structure. The query must be incrementally maintainable.

    - For value indexes: Must include an ORDER BY clause specifying the key columns
    - For aggregate indexes: Must include GROUP BY with a single aggregate function
    - May include WHERE clause for filtered indexes

INDEX ON Parameters
-------------------

``source``
    The name of the table or view to index.

``columns``
    A comma-separated list of column names that form the index key. Each column can optionally specify:

    - Sort order: ``ASC`` (default) or ``DESC``
    - NULL handling: ``NULLS FIRST`` or ``NULLS LAST``

    Example: ``category DESC, price ASC NULLS LAST``

``INCLUDE(valueColumns)`` (optional)
    A comma-separated list of additional columns to store in the index as values (not part of the key).
    This creates a covering index that can satisfy queries without accessing the base table.

``OPTIONS(...)`` (optional)
    Index-specific configuration options. Available options depend on the index type.

Limitations
===========

Both syntaxes share the same limitations because they use the same underlying index implementation:

- Indexes must be incrementally maintainable
- Aggregate indexes support only one aggregate function per index
- Aggregate indexes require integer types for the aggregated column
- The query structure must allow computing index updates from individual record changes
- See :ref:`index_definition` for detailed limitations

See Also
========

- :ref:`create-schema-template` - Schema template definition
- :ref:`index_definition` - Detailed index definition and limitations
- :ref:`CREATE VIEW <create-view>` - Creating views for use with INDEX ON
