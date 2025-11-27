=================
IS DISTINCT FROM
=================

.. _is-distinct-from:

Tests whether two values are distinct (different), treating NULL as a known value.

Syntax
======

.. raw:: html
    :file: IS_DISTINCT_FROM.diagram.svg

The IS DISTINCT FROM operator is used in WHERE clauses:

.. code-block:: sql

    SELECT column1, column2
    FROM table_name
    WHERE column1 IS DISTINCT FROM value

Parameters
==========

``expression1``
    The first value to compare. Can be a column name, calculation, or any valid expression.

``expression2``
    The second value to compare. Can be a column name, calculation, constant, or any valid expression.

``NOT`` (optional)
    Negates the result - returns true if the values are **not** distinct (i.e., they are the same).

Returns
=======

``IS DISTINCT FROM`` returns:
- ``TRUE`` if the values are different OR one is NULL and the other is not
- ``FALSE`` if both values are equal OR both are NULL

``IS NOT DISTINCT FROM`` returns:
- ``TRUE`` if both values are equal OR both are NULL
- ``FALSE`` if the values are different OR one is NULL and the other is not

**Important**: Unlike regular comparison operators (``=``, ``<>``), ``IS DISTINCT FROM`` treats NULL as a comparable value and **never returns NULL**.

Examples
========

Setup
-----

For these examples, assume we have a ``products`` table:

.. code-block:: sql

    CREATE TABLE products(
        id BIGINT,
        name STRING,
        price BIGINT,
        stock INTEGER,
        PRIMARY KEY(id))

    INSERT INTO products VALUES
        (1, 'Widget A', 100, 50),
        (2, 'Widget B', 150, 30),
        (3, 'Gadget X', 200, 20),
        (4, 'Tool A', 80, 100),
        (5, 'Tool B', 120, 15)

IS DISTINCT FROM with Non-NULL Values
--------------------------------------

Find products with prices different from 100:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price IS DISTINCT FROM 100

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Gadget X"`
      - :json:`200`
    * - :json:`"Tool A"`
      - :json:`80`
    * - :json:`"Tool B"`
      - :json:`120`

IS NOT DISTINCT FROM (Equality with NULL Safety)
-------------------------------------------------

Find products with price equal to 100:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price IS NOT DISTINCT FROM 100

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget A"`
      - :json:`100`

This is equivalent to ``WHERE price = 100`` when no NULLs are involved.

Comparing NULL Values
---------------------

Setup with NULL values:

.. code-block:: sql

    CREATE TABLE inventory(
        id BIGINT,
        product_name STRING,
        quantity INTEGER,
        PRIMARY KEY(id))

    INSERT INTO inventory VALUES
        (1, 'Item A', 10),
        (2, 'Item B', NULL),
        (3, 'Item C', NULL)

Find items where quantity is NULL:

.. code-block:: sql

    SELECT product_name, quantity
    FROM inventory
    WHERE quantity IS DISTINCT FROM 10

.. list-table::
    :header-rows: 1

    * - :sql:`product_name`
      - :sql:`quantity`
    * - :json:`"Item B"`
      - :json:`null`
    * - :json:`"Item C"`
      - :json:`null`

Both NULL rows are returned because NULL is considered distinct from 10.

Comparing Two NULL Values
--------------------------

Find items where quantity is NULL:

.. code-block:: sql

    SELECT product_name, quantity
    FROM inventory
    WHERE quantity IS NOT DISTINCT FROM NULL

.. list-table::
    :header-rows: 1

    * - :sql:`product_name`
      - :sql:`quantity`
    * - :json:`"Item B"`
      - :json:`null`
    * - :json:`"Item C"`
      - :json:`null`

This returns rows where quantity is NULL. ``IS NOT DISTINCT FROM NULL`` is equivalent to ``IS NULL``.

Important Notes
===============

NULL Handling
-------------

The key difference between ``IS DISTINCT FROM`` and regular comparison operators:

.. code-block:: sql

    -- Regular comparison with NULL returns NULL (unknown)
    WHERE price = NULL           -- Always returns NULL (no rows match)
    WHERE price <> NULL          -- Always returns NULL (no rows match)

    -- IS DISTINCT FROM with NULL returns TRUE or FALSE (never NULL)
    WHERE price IS DISTINCT FROM NULL      -- Returns TRUE for non-NULL values
    WHERE price IS NOT DISTINCT FROM NULL  -- Returns TRUE for NULL values

Never Returns NULL
------------------

``IS DISTINCT FROM`` always returns a boolean (TRUE or FALSE), never NULL. This makes it suitable for cases where you need deterministic comparison behavior.

Equivalence
-----------

For non-NULL values:

.. code-block:: sql

    -- These are equivalent when x and y are both non-NULL:
    WHERE x IS DISTINCT FROM y
    WHERE x <> y

    -- These are equivalent when x and y are both non-NULL:
    WHERE x IS NOT DISTINCT FROM y
    WHERE x = y

For NULL handling:

.. code-block:: sql

    -- These are equivalent:
    WHERE x IS NOT DISTINCT FROM NULL
    WHERE x IS NULL

    -- These are equivalent:
    WHERE x IS DISTINCT FROM NULL
    WHERE x IS NOT NULL

Comparison with Standard Operators
-----------------------------------

.. list-table::
    :header-rows: 1
    :widths: 30 35 35

    * - Comparison
      - Result with ``=`` / ``<>``
      - Result with ``IS [NOT] DISTINCT FROM``
    * - ``5 vs 5``
      - ``= TRUE``, ``<> FALSE``
      - ``IS DISTINCT FALSE``, ``IS NOT DISTINCT TRUE``
    * - ``5 vs 10``
      - ``= FALSE``, ``<> TRUE``
      - ``IS DISTINCT TRUE``, ``IS NOT DISTINCT FALSE``
    * - ``5 vs NULL``
      - ``= NULL``, ``<> NULL``
      - ``IS DISTINCT TRUE``, ``IS NOT DISTINCT FALSE``
    * - ``NULL vs NULL``
      - ``= NULL``, ``<> NULL``
      - ``IS DISTINCT FALSE``, ``IS NOT DISTINCT TRUE``

Use Cases
---------

``IS DISTINCT FROM`` is particularly useful when:

1. Comparing values that might be NULL
2. Implementing UPSERT or MERGE logic that needs to detect actual changes
3. Writing queries where NULL should be treated as a comparable value
4. Avoiding three-valued logic (TRUE/FALSE/NULL) in comparisons

Type Compatibility
------------------

The two expressions must be of compatible types. The comparison follows SQL type coercion rules.

See Also
========

* :ref:`IS Operators <is-operators>` - IS NULL, IS TRUE, IS FALSE operators
* :ref:`Comparison Operators <comparison-operators>` - Other comparison operations
