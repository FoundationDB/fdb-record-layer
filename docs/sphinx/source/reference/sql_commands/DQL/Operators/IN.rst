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

``expression [NOT] IN (value1, value2, ...)``
    Tests whether an expression matches any value in the provided list.

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
- ``NULL`` if:

  - The expression is NULL, OR
  - The expression doesn't match any non-NULL value AND the list contains at least one NULL

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

IN has special NULL semantics that can be surprising:

1. If the expression is NULL, IN returns NULL:

.. code-block:: sql

    WHERE NULL IN (1, 2, 3)     -- Returns NULL

2. If the list contains NULL and no match is found, IN returns NULL (not FALSE):

.. code-block:: sql

    WHERE 5 IN (1, 2, NULL)     -- Returns NULL (not FALSE)
    WHERE 1 IN (1, 2, NULL)     -- Returns TRUE

3. NOT IN with NULL in the list can produce unexpected results:

.. code-block:: sql

    -- Be careful with NOT IN when NULLs might be present
    WHERE 5 NOT IN (1, 2, NULL) -- Returns NULL (not TRUE!)

To avoid NULL-related issues with NOT IN, consider filtering NULLs or using alternative approaches.

Equivalence
-----------

IN is shorthand for multiple OR comparisons:

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
