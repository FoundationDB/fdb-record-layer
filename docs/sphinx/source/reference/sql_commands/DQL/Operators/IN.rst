==
IN
==

.. _in:

Tests whether a value matches any value in a list.

Syntax
======

.. raw:: html
    :file: IN.diagram.svg

The IN operator is used in WHERE clauses:

.. code-block:: sql

    SELECT column1, column2
    FROM table_name
    WHERE column1 IN (value1, value2, value3)

Parameters
==========

``expression``
    The value to test. Can be a column name, calculation, or any valid expression.

``value1, value2, ...``
    A comma-separated list of values to compare against. Values must be of compatible types with the expression.

``NOT`` (optional)
    Negates the result - returns true if the expression does **not** match any value in the list.

Returns
=======

Returns:

- ``TRUE`` if the expression equals any value in the list
- ``FALSE`` if the expression does not match any value in the list
- ``NULL`` if the expression is ``NULL``

A ``NULL`` in the list itself is not supported. See `NULL Handling`_ below.

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

    INSERT INTO products VALUES
        (1, 'Widget A', 'Electronics', 100),
        (2, 'Widget B', 'Electronics', 150),
        (3, 'Gadget X', 'Electronics', 200),
        (4, 'Tool A', 'Hardware', 80),
        (5, 'Tool B', 'Hardware', 120),
        (6, 'Book A', 'Media', 25),
        (7, 'Book B', 'Media', 30)

IN with Numbers
---------------

Find products with specific IDs:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE id IN (1, 3, 5)

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Gadget X"`
      - :json:`200`
    * - :json:`"Tool B"`
      - :json:`120`

IN with Strings
---------------

Find products in specific categories:

.. code-block:: sql

    SELECT name, category
    FROM products
    WHERE category IN ('Electronics', 'Media')

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`category`
    * - :json:`"Widget A"`
      - :json:`"Electronics"`
    * - :json:`"Widget B"`
      - :json:`"Electronics"`
    * - :json:`"Gadget X"`
      - :json:`"Electronics"`
    * - :json:`"Book A"`
      - :json:`"Media"`
    * - :json:`"Book B"`
      - :json:`"Media"`

NOT IN
------

Find products not in specific categories:

.. code-block:: sql

    SELECT name, category
    FROM products
    WHERE category NOT IN ('Electronics', 'Media')

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`category`
    * - :json:`"Tool A"`
      - :json:`"Hardware"`
    * - :json:`"Tool B"`
      - :json:`"Hardware"`

Single Value IN
---------------

IN with a single value is equivalent to ``=``:

.. code-block:: sql

    -- These are equivalent:
    WHERE category IN ('Hardware')
    WHERE category = 'Hardware'

Empty List
----------

IN with an empty list always returns FALSE:

.. code-block:: sql

    SELECT * FROM products WHERE id IN ()
    -- Returns no rows

Important Notes
===============

NULL Handling
-------------

If the expression is ``NULL``, ``IN`` returns ``NULL``, so the row is not returned:

.. code-block:: sql

    WHERE NULL IN (1, 2, 3)     -- Returns NULL

A ``NULL`` in the list itself is *not supported*. The Relational Layer represents an ``IN`` list as an array, and an array cannot hold a ``NULL`` element, so the query raises a :sql:`WRONG_OBJECT_TYPE` error (error code ``42809``):

.. code-block:: sql

    -- ERROR: NULL values are not allowed in the IN list
    WHERE 5 IN (1, 2, NULL)
    WHERE 5 NOT IN (1, 2, NULL)

Only a ``NULL`` written directly in the list is rejected before the query runs. An element with a resolved type is accepted, even if its value turns out to be ``NULL`` while the query runs. Such a query raises an :sql:`UNSUPPORTED_OPERATION` error (error code ``0A000``) during execution:

.. code-block:: sql

    -- ERROR: An ARRAY value cannot have NULL elements
    WHERE a IN (1, CAST(NULL AS BIGINT))  -- fails at runtime
    WHERE a IN (1, nullable_column)       -- fails at runtime if the column is NULL for some row

Equivalence
-----------

IN is shorthand for multiple comparisons:

.. code-block:: sql

    -- These are equivalent:
    WHERE x IN (1, 2, 3)
    WHERE x = 1 OR x = 2 OR x = 3

    -- These are equivalent:
    WHERE x NOT IN (1, 2, 3)
    WHERE x != 1 AND x != 2 AND x != 3

Type Compatibility
------------------

All values in the IN list must be of compatible types with the expression. Mixing incompatible types will result in a type error:

.. code-block:: sql

    -- ERROR: Type mismatch
    WHERE id IN (1, 'two', 3)

Performance Considerations
--------------------------

IN is most efficient with small, static value lists. For large value lists or dynamic values, consider:

- Using a JOIN with a separate table
- Creating an appropriate index
- Using other filtering strategies

Subqueries
----------

IN does **not** currently support subqueries:

.. code-block:: sql

    -- NOT SUPPORTED:
    WHERE category IN (SELECT category FROM popular_categories)

Use JOINs or other techniques for set-based filtering.

See Also
========

* :ref:`Comparison Operators <comparison-operators>` - Other comparison operations
