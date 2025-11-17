========
ORDER BY
========

.. _order_by:

Sorts query results by one or more expressions in ascending or descending order.

Syntax
======

.. raw:: html
    :file: ORDER_BY.diagram.svg

The ORDER BY clause is used in SELECT statements:

.. code-block:: sql

    SELECT column1, column2
    FROM table_name
    ORDER BY column1 ASC, column2 DESC

Parameters
==========

``ORDER BY expression [ASC|DESC] [NULLS FIRST|NULLS LAST], ...``
    Sorts rows based on the values of one or more expressions. Results are ordered by the first expression, then by the second expression for rows with equal first expression values, and so on.

``expression``
    Can be:

    - Column names
    - Nested field references (e.g., ``struct_column.field``)
    - Columns not in the SELECT list

``ASC`` (optional, default)
    Sort in ascending order (smallest to largest)

``DESC`` (optional)
    Sort in descending order (largest to smallest)

``NULLS FIRST`` (optional)
    Place NULL values at the beginning of the result set

``NULLS LAST`` (optional)
    Place NULL values at the end of the result set

By default, NULL values sort as follows:
- With ``ASC``: NULLs come last (equivalent to ``ASC NULLS LAST``)
- With ``DESC``: NULLs come first (equivalent to ``DESC NULLS FIRST``)

Returns
=======

Returns all selected rows sorted according to the specified order. The order is guaranteed to be stable and deterministic.

Examples
========

Setup
-----

For these examples, assume we have a ``products`` table:

.. code-block:: sql

    CREATE TABLE products(
        id BIGINT,
        name STRING,
        category STRING,
        price BIGINT,
        PRIMARY KEY(id))

    CREATE INDEX price_idx AS SELECT price, category FROM products ORDER BY price
    CREATE INDEX category_idx AS SELECT category, price FROM products ORDER BY category
    CREATE INDEX category_price_idx AS SELECT category, price FROM products ORDER BY category, price

    INSERT INTO products VALUES
        (1, 'Widget A', 'Electronics', 100),
        (2, 'Widget B', 'Electronics', 150),
        (3, 'Gadget X', 'Electronics', 200),
        (4, 'Tool A', 'Hardware', 80),
        (5, 'Tool B', 'Hardware', 120)

ORDER BY Single Column
-----------------------

Sort products by price in ascending order:

.. code-block:: sql

    SELECT name, price
    FROM products
    ORDER BY price

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Tool A"`
      - :json:`80`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Tool B"`
      - :json:`120`
    * - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Gadget X"`
      - :json:`200`

ORDER BY DESC
--------------

Sort products by price in descending order:

.. code-block:: sql

    SELECT name, price
    FROM products
    ORDER BY price DESC

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Gadget X"`
      - :json:`200`
    * - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Tool B"`
      - :json:`120`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Tool A"`
      - :json:`80`

ORDER BY Multiple Columns
---------------------------

Sort by category, then by price within each category:

.. code-block:: sql

    SELECT category, name, price
    FROM products
    ORDER BY category, price

.. list-table::
    :header-rows: 1

    * - :sql:`category`
      - :sql:`name`
      - :sql:`price`
    * - :json:`"Electronics"`
      - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Electronics"`
      - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Electronics"`
      - :json:`"Gadget X"`
      - :json:`200`
    * - :json:`"Hardware"`
      - :json:`"Tool A"`
      - :json:`80`
    * - :json:`"Hardware"`
      - :json:`"Tool B"`
      - :json:`120`

ORDER BY with Mixed Directions
--------------------------------

Sort by category ascending, price descending:

.. code-block:: sql

    SELECT category, name, price
    FROM products
    ORDER BY category ASC, price DESC

This query requires an index with matching sort order: ``ORDER BY category, price DESC``

ORDER BY with WHERE
--------------------

Combine filtering with sorting:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price >= 100
    ORDER BY price

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Tool B"`
      - :json:`120`
    * - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Gadget X"`
      - :json:`200`

ORDER BY Non-Projected Column
------------------------------

You can order by columns not in the SELECT list:

.. code-block:: sql

    SELECT name
    FROM products
    ORDER BY price

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Tool A"`
    * - :json:`"Widget A"`
    * - :json:`"Tool B"`
    * - :json:`"Widget B"`
    * - :json:`"Gadget X"`

The query planner will use an index that includes the ordering column, even if it's not projected in the result.

ORDER BY on Nested Fields
--------------------------

For tables with struct types, you can order by nested fields:

.. code-block:: sql

    CREATE TYPE AS STRUCT address_type(city STRING, zipcode INTEGER)
    CREATE TABLE customers(
        id BIGINT,
        name STRING,
        address address_type,
        PRIMARY KEY(id))

    CREATE INDEX city_idx AS SELECT address.city FROM customers ORDER BY address.city

    SELECT name, address.city
    FROM customers
    ORDER BY address.city

NULL Handling
--------------

By default, NULL values have specific sort positions:

- With ``ASC``: NULL values appear **last**
- With ``DESC``: NULL values appear **first**

You can override this behavior with ``NULLS FIRST`` or ``NULLS LAST``:

.. code-block:: sql

    -- NULLs at the beginning (overriding ASC default)
    SELECT name, rating
    FROM products
    ORDER BY rating ASC NULLS FIRST

    -- NULLs at the end (overriding DESC default)
    SELECT name, rating
    FROM products
    ORDER BY rating DESC NULLS LAST

**Note**: Using ``NULLS FIRST`` or ``NULLS LAST`` requires an index with matching NULL ordering, similar to the constraint for mixed ASC/DESC ordering.

Important Notes
===============

Index Requirement
-----------------

**ORDER BY operations require an index with matching sort order.** FRL does not perform in-memory sorting. The query planner must find an index that satisfies the ordering requirement.

Example index for ``ORDER BY price``:

.. code-block:: sql

    CREATE INDEX price_idx AS SELECT price FROM products ORDER BY price

Without a suitable index, the query will fail with an "unable to plan" error (0AF00).

See :ref:`Indexes <index_definition>` for details on creating indexes that support ORDER BY operations.

Mixed Ordering Constraints
---------------------------

Mixed ordering (e.g., ``ORDER BY a ASC, b DESC``) is only supported if a matching index exists with that exact ordering:

.. code-block:: sql

    -- This requires an index: ORDER BY category ASC, price DESC
    SELECT * FROM products ORDER BY category ASC, price DESC

Without a matching index, mixed ordering queries will fail with error 0AF00.

To create a matching index:

.. code-block:: sql

    CREATE INDEX cat_price_desc_idx AS
        SELECT category, price FROM products
        ORDER BY category ASC, price DESC

Subquery Restrictions
---------------------

ORDER BY is **not allowed in subqueries** or nested SELECT statements:

.. code-block:: sql

    -- ERROR: ORDER BY in subquery not allowed
    SELECT * FROM (SELECT * FROM products ORDER BY price) AS sub

    -- ERROR: ORDER BY in EXISTS subquery not allowed
    SELECT * FROM products WHERE EXISTS
        (SELECT * FROM products ORDER BY price LIMIT 1)

This restriction is due to the architecture's requirement for index-backed operations.

Pagination
----------

For large result sets, use JDBC's ``maxRows`` parameter for pagination with continuations:

.. code-block:: java

    Statement stmt = conn.createStatement();
    stmt.setMaxRows(10);  // Fetch 10 rows at a time
    ResultSet rs = stmt.executeQuery("SELECT * FROM products ORDER BY price");

Continuations allow stateless pagination without LIMIT/OFFSET syntax.

**Note**: SQL ``LIMIT ... OFFSET`` syntax is not supported. Use JDBC's maxRows parameter instead.

Execution Model
---------------

FRL does not perform in-memory sorting. All ORDER BY operations must be backed by an index with compatible ordering. This is a fundamental architectural constraint that ensures queries can execute efficiently over large datasets.

The query planner will:
1. Look for an index with matching sort order
2. Use that index to scan results in the correct order
3. Fail with error 0AF00 if no suitable index exists

See Also
========

* :ref:`Indexes <index_definition>` - Creating indexes for ORDER BY
