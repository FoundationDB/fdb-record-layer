=======
BETWEEN
=======

.. _between:

Tests whether a value falls within a specified range (inclusive).

Syntax
======

.. raw:: html
    :file: BETWEEN.diagram.svg

The BETWEEN operator is used in WHERE clauses:

.. code-block:: sql

    SELECT column1, column2
    FROM table_name
    WHERE column1 BETWEEN lower_value AND upper_value

Parameters
==========

``expression [NOT] BETWEEN lower_bound AND upper_bound``
    Tests whether an expression's value is within the inclusive range ``[lower_bound, upper_bound]``.

``expression``
    The value to test. Can be a column name, calculation, or any valid expression.

``lower_bound``
    The lower bound of the range (inclusive).

``upper_bound``
    The upper bound of the range (inclusive).

``NOT`` (optional)
    Negates the result - returns true if the value is **outside** the range.

Returns
=======

Returns:
- ``TRUE`` if ``expression >= lower_bound AND expression <= upper_bound``
- ``FALSE`` otherwise
- ``NULL`` if any operand is NULL

With ``NOT BETWEEN``:
- ``TRUE`` if ``expression < lower_bound OR expression > upper_bound``
- ``FALSE`` otherwise
- ``NULL`` if any operand is NULL

**Important**: If ``lower_bound > upper_bound``, the range is empty and ``BETWEEN`` always returns ``FALSE``.

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

BETWEEN with Numbers
--------------------

Find products with prices between 100 and 150 (inclusive):

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price BETWEEN 100 AND 150

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Widget B"`
      - :json:`150`
    * - :json:`"Tool B"`
      - :json:`120`

NOT BETWEEN
-----------

Find products with prices outside the range 100-150:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price NOT BETWEEN 100 AND 150

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Tool A"`
      - :json:`80`
    * - :json:`"Gadget X"`
      - :json:`200`

BETWEEN with Equal Bounds
--------------------------

Test for exact value using BETWEEN:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price BETWEEN 100 AND 100

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Widget A"`
      - :json:`100`

This is equivalent to ``WHERE price = 100``.

Empty Range
-----------

If lower bound > upper bound, no rows match:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price BETWEEN 150 AND 100

Returns empty result set (no rows).

Combined with OR
----------------

Use multiple BETWEEN clauses with OR:

.. code-block:: sql

    SELECT name, price
    FROM products
    WHERE price BETWEEN 80 AND 100 OR price BETWEEN 180 AND 220

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
    * - :json:`"Tool A"`
      - :json:`80`
    * - :json:`"Widget A"`
      - :json:`100`
    * - :json:`"Gadget X"`
      - :json:`200`

Important Notes
===============

Inclusive Range
---------------

``BETWEEN`` uses **inclusive** bounds. Both ``lower_bound`` and ``upper_bound`` are included in the matching range.

NULL Handling
-------------

If any operand (expression, lower_bound, or upper_bound) is NULL, the result is NULL:

.. code-block:: sql

    -- Returns NULL
    WHERE NULL BETWEEN 1 AND 10

    -- Returns NULL
    WHERE price BETWEEN NULL AND 100

    -- Returns NULL
    WHERE price BETWEEN 100 AND NULL

Equivalence
-----------

``BETWEEN`` is shorthand for a range check:

.. code-block:: sql

    -- These are equivalent:
    WHERE x BETWEEN a AND b
    WHERE x >= a AND x <= b

    -- These are equivalent:
    WHERE x NOT BETWEEN a AND b
    WHERE x < a OR x > b

Type Compatibility
------------------

The expression, lower_bound, and upper_bound must be of compatible types. The comparison follows SQL type coercion rules.

See Also
========

* :ref:`Comparison Operators <comparison-operators>` - Other comparison operations
