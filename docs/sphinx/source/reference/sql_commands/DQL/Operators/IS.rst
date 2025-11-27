===========
IS Operator
===========

.. _is-operators:

Tests for NULL values or boolean values (TRUE/FALSE).

Syntax
======

.. raw:: html
    :file: IS.diagram.svg

The IS operator is used in WHERE clauses:

.. code-block:: sql

    SELECT column1, column2
    FROM table_name
    WHERE column1 IS NULL

    SELECT column1, column2
    FROM table_name
    WHERE boolean_column IS TRUE

Parameters
==========

``expression``
    The value to test. Can be a column name, calculation, or any valid expression.

``NULL`` / ``NOT NULL``
    Tests whether the expression is NULL (or not NULL).

``TRUE`` / ``NOT TRUE``
    Tests whether a boolean expression is TRUE (or not TRUE).

``FALSE`` / ``NOT FALSE``
    Tests whether a boolean expression is FALSE (or not FALSE).

Returns
=======

All IS operators return a boolean value (TRUE or FALSE), never NULL:

**IS NULL**:
- ``TRUE`` if the expression is NULL
- ``FALSE`` if the expression is not NULL

**IS NOT NULL**:
- ``TRUE`` if the expression is not NULL
- ``FALSE`` if the expression is NULL

**IS TRUE**:
- ``TRUE`` if the expression is TRUE
- ``FALSE`` if the expression is FALSE or NULL

**IS NOT TRUE**:
- ``TRUE`` if the expression is FALSE or NULL
- ``FALSE`` if the expression is TRUE

**IS FALSE**:
- ``TRUE`` if the expression is FALSE
- ``FALSE`` if the expression is TRUE or NULL

**IS NOT FALSE**:
- ``TRUE`` if the expression is TRUE or NULL
- ``FALSE`` if the expression is FALSE

Examples
========

Setup
-----

For these examples, assume we have a ``products`` table with some NULL values:

.. code-block:: sql

    CREATE TABLE products(
        id BIGINT,
        name STRING,
        category STRING,
        price BIGINT,
        in_stock BOOLEAN,
        PRIMARY KEY(id))

    INSERT INTO products VALUES
        (1, 'Widget A', 'Electronics', 100, true),
        (2, 'Widget B', NULL, 150, false),
        (3, 'Gadget X', 'Electronics', NULL, true),
        (4, 'Tool A', 'Hardware', 80, NULL),
        (5, 'Tool B', NULL, NULL, NULL)

IS NULL
-------

Find products with no category:

.. code-block:: sql

    SELECT name, category
    FROM products
    WHERE category IS NULL

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`category`
    * - :json:`"Widget B"`
      - :json:`null`
    * - :json:`"Tool B"`
      - :json:`null`

IS NOT NULL
-----------

Find products that have a price:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price IS NOT NULL

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Tool A"`
      - :json:`80`

IS TRUE
-------

Find products that are in stock:

.. code-block:: sql

    SELECT name, in_stock
    FROM products
    WHERE in_stock IS TRUE

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`in_stock`
    * - :json:`"Widget A"`
      - :json:`true`
    * - :json:`"Gadget X"`
      - :json:`true`

Note: Rows where ``in_stock`` is NULL are not returned.

IS NOT TRUE
-----------

Find products that are either out of stock or have unknown stock status:

.. code-block:: sql

    SELECT name, in_stock
    FROM products
    WHERE in_stock IS NOT TRUE

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`in_stock`
    * - :json:`"Widget B"`
      - :json:`false`
    * - :json:`"Tool A"`
      - :json:`null`
    * - :json:`"Tool B"`
      - :json:`null`

This returns rows where ``in_stock`` is either FALSE or NULL.

IS FALSE
--------

Find products that are explicitly out of stock:

.. code-block:: sql

    SELECT name, in_stock
    FROM products
    WHERE in_stock IS FALSE

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`in_stock`
    * - :json:`"Widget B"`
      - :json:`false`

Note: Rows where ``in_stock`` is NULL are not returned.

IS NOT FALSE
------------

Find products that are either in stock or have unknown stock status:

.. code-block:: sql

    SELECT name, in_stock
    FROM products
    WHERE in_stock IS NOT FALSE

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`in_stock`
    * - :json:`"Widget A"`
      - :json:`true`
    * - :json:`"Gadget X"`
      - :json:`true`
    * - :json:`"Tool A"`
      - :json:`null`
    * - :json:`"Tool B"`
      - :json:`null`

This returns rows where ``in_stock`` is either TRUE or NULL.

Combining IS NULL with Other Conditions
----------------------------------------

Find products with no category and a price over 100:

.. code-block:: sql

    SELECT name, category, price
    FROM products
    WHERE category IS NULL AND price > 100

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`category`
      - :sql:`price`
    * - :json:`"Widget B"`
      - :json:`null`
      - :json:`150`

Important Notes
===============

IS NULL vs = NULL
-----------------

You **must** use ``IS NULL`` to test for NULL values. Using ``= NULL`` does not work:

.. code-block:: sql

    -- CORRECT: Returns rows where price is NULL
    WHERE price IS NULL

    -- WRONG: Always returns no rows (NULL = NULL evaluates to NULL, not TRUE)
    WHERE price = NULL

Boolean IS Operators and NULL Handling
---------------------------------------

The ``IS TRUE`` and ``IS FALSE`` operators treat NULL as a third state:

.. code-block:: sql

    -- For a boolean column with value NULL:
    column IS TRUE       -- Returns FALSE
    column IS NOT TRUE   -- Returns TRUE
    column IS FALSE      -- Returns FALSE
    column IS NOT FALSE  -- Returns TRUE
    column IS NULL       -- Returns TRUE

Equivalences for Boolean Operators
-----------------------------------

The following equivalences hold for boolean expressions:

.. code-block:: sql

    -- IS NOT TRUE is equivalent to:
    x IS NOT TRUE
    x IS NULL OR x = FALSE

    -- IS NOT FALSE is equivalent to:
    x IS NOT FALSE
    x IS NULL OR x = TRUE

Truth Tables
------------

For a boolean column ``b``, here are the results of IS operators:

.. list-table::
    :header-rows: 1
    :widths: 20 20 20 20 20

    * - ``b`` value
      - ``IS TRUE``
      - ``IS NOT TRUE``
      - ``IS FALSE``
      - ``IS NOT FALSE``
    * - ``TRUE``
      - ``TRUE``
      - ``FALSE``
      - ``FALSE``
      - ``TRUE``
    * - ``FALSE``
      - ``FALSE``
      - ``TRUE``
      - ``TRUE``
      - ``FALSE``
    * - ``NULL``
      - ``FALSE``
      - ``TRUE``
      - ``FALSE``
      - ``TRUE``

Use Cases
---------

**IS NULL / IS NOT NULL**:
- Filtering rows with missing data
- Data quality checks
- Handling optional fields

**IS TRUE / IS FALSE**:
- Explicit boolean checks when NULL values need special handling
- Feature flags with unknown states
- Three-valued logic in business rules

**IS NOT TRUE / IS NOT FALSE**:
- Treating NULL as equivalent to FALSE (for IS NOT TRUE)
- Treating NULL as equivalent to TRUE (for IS NOT FALSE)
- Default value assumptions for unset boolean fields

Type Restrictions
-----------------

- ``IS NULL`` and ``IS NOT NULL`` work with all types
- ``IS TRUE``, ``IS FALSE``, ``IS NOT TRUE``, and ``IS NOT FALSE`` only work with **BOOLEAN** expressions

Attempting to use boolean IS operators on non-boolean types will result in a type error:

.. code-block:: sql

    -- ERROR: price is BIGINT, not BOOLEAN
    WHERE price IS TRUE

See Also
========

* :ref:`IS DISTINCT FROM <is-distinct-from>` - NULL-safe comparison operator
* :ref:`Comparison Operators <comparison-operators>` - Other comparison operations
