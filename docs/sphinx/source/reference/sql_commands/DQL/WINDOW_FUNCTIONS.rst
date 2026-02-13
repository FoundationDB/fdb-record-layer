================
Window Functions
================

.. _window_functions:

Window functions perform calculations across a set of rows that are related to the current row. Unlike aggregate functions, window functions do not collapse rows into a single result but instead return a value for each row.

The Relational Layer currently supports the ``ROW_NUMBER()`` window function for semantic search operations using HNSW (Hierarchical Navigable Small World) vector indexes. When combined with distance functions and backed by an HNSW index, ``ROW_NUMBER()`` enables efficient K-nearest neighbor (KNN) searches.

.. important::

   Currently, only the ``ROW_NUMBER()`` window function is supported for HNSW-backed semantic search. Other window functions (``RANK()``, ``DENSE_RANK()``, ``LAG()``, ``LEAD()``, etc.) are not yet supported in this context. Additionally, ``ROW_NUMBER()`` requires a proper HNSW vector index to function correctly for semantic search queries.

.. important::

   Window functions are **not allowed** in the ``WHERE`` clause, as per SQL standard. Use the ``QUALIFY`` clause to filter on window function results. See `The QUALIFY Clause`_ for more details.

Syntax
======

.. raw:: html
    :file: WINDOW_FUNCTIONS.diagram.svg

Window functions follow this general syntax:

.. code-block:: sql

    window_function([arguments])
    OVER (
        [PARTITION BY column1, column2, ...]
        [ORDER BY expression [ASC | DESC]]
        [OPTIONS option1 = value1, option2 = value2, ...]
    )

Components
==========

``window_function``
    The window function to apply. Currently, only ``ROW_NUMBER()`` is supported for HNSW-backed semantic search:

    - ``ROW_NUMBER()`` - Assigns a sequential integer to each row within the partition based on the specified ordering. Must be backed by an HNSW vector index when used for semantic search.

``PARTITION BY`` (optional)
    Divides the result set into partitions to which the window function is applied. Each partition is processed independently.

``ORDER BY`` (optional)
    Defines the logical order of rows within each partition. For semantic search, this typically orders by a distance function.

``OPTIONS`` (optional)
    Specifies additional parameters for the window function. For HNSW-based semantic search:

    - ``ef_search`` - Controls the size of the dynamic candidate list during search. Higher values improve recall but increase query time. Default is typically set at index creation time.

Semantic Search with HNSW
==========================

The primary use case for window functions in the Relational Layer is semantic search using HNSW vector indexes. The ``ROW_NUMBER()`` function, combined with distance metrics and filtering, enables efficient K-nearest neighbor searches.

How It Works
------------

1. An HNSW vector index is created on a table with vector columns
2. The index is partitioned by specified columns (e.g., zone, category)
3. Queries use ``ROW_NUMBER() OVER (PARTITION BY ... ORDER BY distance_function(...))`` to rank results
4. A ``QUALIFY`` clause with ``ROW_NUMBER() <= K`` limits results to the top K nearest neighbors
5. The query planner recognizes this pattern and uses the HNSW index for efficient retrieval

Supported Distance Functions
-----------------------------

The following distance functions are available for use with HNSW indexes:

- ``euclidean_distance(vector1, vector2)`` - Euclidean (L2) distance
- ``euclidean_square_distance(vector1, vector2)`` - Squared Euclidean distance (more efficient, preserves ordering)
- ``cosine_distance(vector1, vector2)`` - Cosine distance (1 - cosine similarity)
- ``dot_product_distance(vector1, vector2)`` - Negative dot product (for maximum inner product search)

.. note::

   The distance function used in queries must match the metric specified when creating the HNSW index. For example, if the index was created with ``metric = euclidean_metric``, use the ``euclidean_distance()`` function in queries. See :ref:`vector_index_syntax` for more information about creating vector indexes and supported metrics.

Limitations
-----------

- **Window functions not allowed in WHERE clause**: As per SQL standard, window functions cannot be used in the ``WHERE`` clause. Use the ``QUALIFY`` clause instead to filter on window function results
- **Only ROW_NUMBER() is supported**: Other window functions (``RANK()``, ``DENSE_RANK()``, ``LAG()``, ``LEAD()``, etc.) are not yet supported for HNSW-backed semantic search
- **Requires HNSW index**: ``ROW_NUMBER()`` must be backed by a proper HNSW vector index on the queried table
- **Limited comparison operators**: Only ``<`` and ``<=`` comparisons are supported for filtering ``ROW_NUMBER()`` results; the ``=`` comparison is not yet supported
- **Distance function must match index metric**: The distance function in ``ORDER BY`` must match the metric specified when creating the HNSW index

Examples
========

Setup
-----

For these examples, assume we have a document store with embeddings:

.. code-block:: sql

    CREATE TABLE documents(
        zone STRING,
        docId STRING,
        bookshelf STRING,
        title STRING,
        embedding VECTOR(3, HALF),
        PRIMARY KEY (zone, docId))

    CREATE VIEW documentsView AS
        SELECT embedding, zone, bookshelf, docId, title
        FROM documents

    CREATE VECTOR INDEX documentsEuclideanIndex
        USING HNSW
        ON documentsView(embedding)
        PARTITION BY(zone, bookshelf)
        OPTIONS (metric = euclidean_metric)

    INSERT INTO documents VALUES
        ('zone1', 'd1', 'fiction', 'The Great Gatsby', CAST([1.0, 0.0, 0.0] AS VECTOR(3, HALF))),
        ('zone1', 'd2', 'fiction', '1984', CAST([0.9, 0.1, 0.0] AS VECTOR(3, HALF))),
        ('zone1', 'd3', 'fiction', 'To Kill a Mockingbird', CAST([0.8, 0.2, 0.0] AS VECTOR(3, HALF))),
        ('zone1', 'd6', 'science', 'A Brief History of Time', CAST([0.0, 1.0, 0.0] AS VECTOR(3, HALF))),
        ('zone1', 'd7', 'science', 'The Selfish Gene', CAST([0.1, 0.9, 0.0] AS VECTOR(3, HALF)))

The QUALIFY Clause
------------------

The ``QUALIFY`` clause is used to filter rows based on window function results. It is similar to ``WHERE``, but is evaluated after window functions are computed. According to SQL standard, window functions are **not allowed** in the ``WHERE`` clause. Use ``QUALIFY`` instead.

**Syntax:**

.. code-block:: sql

    SELECT columns
    FROM table
    WHERE regular_conditions
    QUALIFY window_function_condition

Basic K-Nearest Neighbor Search
--------------------------------

Find the single closest document in the fiction bookshelf:

.. code-block:: sql

    SELECT docId, title, euclidean_distance(embedding, CAST([1.0, 0.0, 0.0] AS VECTOR(3, HALF))) AS distance
    FROM documents
    WHERE zone = 'zone1' AND bookshelf = 'fiction'
    QUALIFY ROW_NUMBER() OVER (
          PARTITION BY zone, bookshelf
          ORDER BY euclidean_distance(embedding, CAST([1.0, 0.0, 0.0] AS VECTOR(3, HALF))) ASC
      ) <= 1

.. list-table::
    :header-rows: 1

    * - :sql:`docId`
      - :sql:`title`
      - :sql:`distance`
    * - :json:`"d1"`
      - :json:`"The Great Gatsby"`
      - :json:`0.0`

Top-K Search
------------

Find the top 3 most similar documents:

.. code-block:: sql

    SELECT docId, euclidean_distance(embedding, CAST([1.0, 0.0, 0.0] AS VECTOR(3, HALF))) AS distance
    FROM documents
    WHERE zone = 'zone1' AND bookshelf = 'fiction'
    QUALIFY ROW_NUMBER() OVER (
          PARTITION BY zone, bookshelf
          ORDER BY euclidean_distance(embedding, CAST([1.0, 0.0, 0.0] AS VECTOR(3, HALF))) ASC
      ) <= 3

.. list-table::
    :header-rows: 1

    * - :sql:`docId`
      - :sql:`distance`
    * - :json:`"d1"`
      - :json:`0.0`
    * - :json:`"d2"`
      - :json:`0.14147317261689443`
    * - :json:`"d3"`
      - :json:`0.28294634523378887`

Using Custom Search Parameters
-------------------------------

Control the search quality with ``ef_search``:

.. code-block:: sql

    SELECT docId
    FROM documents
    WHERE zone = 'zone1' AND bookshelf = 'science'
    QUALIFY ROW_NUMBER() OVER (
          PARTITION BY zone, bookshelf
          ORDER BY euclidean_distance(embedding, CAST([0.0, 1.0, 0.0] AS VECTOR(3, HALF))) ASC
          OPTIONS ef_search = 100
      ) <= 2

.. list-table::
    :header-rows: 1

    * - :sql:`docId`
    * - :json:`"d6"`
    * - :json:`"d7"`

Using Less Than
---------------

You can use ``<`` instead of ``<=`` to exclude the K-th result:

.. code-block:: sql

    SELECT docId
    FROM documents
    WHERE zone = 'zone1' AND bookshelf = 'science'
    QUALIFY ROW_NUMBER() OVER (
          PARTITION BY zone, bookshelf
          ORDER BY euclidean_distance(embedding, CAST([0.0, 1.0, 0.0] AS VECTOR(3, HALF))) ASC
          OPTIONS ef_search = 200
      ) < 3

.. list-table::
    :header-rows: 1

    * - :sql:`docId`
    * - :json:`"d6"`
    * - :json:`"d7"`

Combining Multiple HNSW Searches
---------------------------------

Use ``OR`` to combine results from different similarity searches:

.. code-block:: sql

    SELECT title
    FROM documents
    WHERE zone = 'zone1' AND bookshelf = 'fiction'
    QUALIFY ROW_NUMBER() OVER (
              PARTITION BY zone, bookshelf
              ORDER BY cosine_distance(embedding, CAST([1.0, 0.0, 0.0] AS VECTOR(3, HALF))) ASC
          ) <= 1
          OR ROW_NUMBER() OVER (
              PARTITION BY zone, bookshelf
              ORDER BY euclidean_distance(embedding, CAST([0.5, 0.5, 0.5] AS VECTOR(3, HALF))) ASC
          ) <= 1

This finds documents that are either the closest to ``[1.0, 0.0, 0.0]`` using cosine distance OR the closest to ``[0.5, 0.5, 0.5]`` using euclidean distance.

Important Notes
===============

Query Planning
--------------

The query planner automatically recognizes the pattern of ``QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY distance_function(...)) <= K`` and uses the HNSW index when:

1. An HNSW index exists on the table
2. The ``PARTITION BY`` columns match the index partition
3. The ``ORDER BY`` uses a distance function matching the index metric
4. The ``QUALIFY`` clause uses ``ROW_NUMBER() <= K`` or ``ROW_NUMBER() < K``

When these conditions are met, the query plan will show ``ISCAN(...BY_DISTANCE)`` indicating efficient index usage.

Performance Considerations
--------------------------

- **ef_search parameter**: Higher values increase recall (accuracy) but reduce performance. Tune this based on your accuracy requirements.
- **Partition size**: Smaller partitions (more specific ``PARTITION BY`` columns) generally perform better.
- **Index metrics**: Ensure the distance function in your query matches the metric specified when creating the HNSW index.

NULL Handling
-------------

- NULL vectors are not indexed and will not appear in HNSW search results
- Distance functions (``euclidean_distance``, ``cosine_distance``, etc.) throw a ``RecordCoreException`` when invoked with NULL vectors
- Ensure vector columns are properly initialized before using them in distance calculations

See Also
========

* :ref:`vector_index_syntax` - Creating HNSW vector indexes and supported metrics
* :ref:`Vector Types <vector_types>` - Working with vector data types
* :ref:`ORDER BY <order_by>` - Ordering query results
