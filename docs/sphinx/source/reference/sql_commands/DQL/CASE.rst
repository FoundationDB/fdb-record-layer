====
CASE
====

.. _case:

Returns a result based on conditional evaluation, similar to if-then-else logic.

Syntax
======

.. raw:: html
    :file: CASE.diagram.svg

CASE expressions use conditional evaluation:

.. code-block:: sql

    CASE
        WHEN condition1 THEN result1
        WHEN condition2 THEN result2
        ...
        ELSE default_result
    END

Parameters
==========

``condition``
    A boolean expression evaluated for each WHEN clause.

``result``
    The value to return if the corresponding condition is TRUE. Can be any expression, column, or literal.

``ELSE result`` (optional)
    The default value returned if no conditions are TRUE. If omitted and no conditions match, returns NULL.

Returns
=======

Returns the result of the first matching WHEN clause, or the ELSE value if no matches are found, or NULL if no ELSE clause is specified and no matches are found.

All result expressions must be of compatible types (or NULL).

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
        stock INTEGER,
        PRIMARY KEY(id))

    INSERT INTO products VALUES
        (1, 'Widget A', 'Electronics', 100, 50),
        (2, 'Widget B', 'Electronics', 150, 5),
        (3, 'Gadget X', 'Electronics', 200, 0),
        (4, 'Tool A', 'Hardware', 80, 100),
        (5, 'Tool B', 'Hardware', 120, 15)

Simple Condition
----------------

Categorize products by stock level:

.. code-block:: sql

    SELECT name,
           stock,
           CASE
               WHEN stock = 0 THEN 'Out of Stock'
               WHEN stock < 10 THEN 'Low Stock'
               WHEN stock < 50 THEN 'In Stock'
               ELSE 'Well Stocked'
           END AS stock_status
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`stock`
      - :sql:`stock_status`
    * - :json:`"Widget A"`
      - :json:`50`
      - :json:`"Well Stocked"`
    * - :json:`"Widget B"`
      - :json:`5`
      - :json:`"Low Stock"`
    * - :json:`"Gadget X"`
      - :json:`0`
      - :json:`"Out of Stock"`
    * - :json:`"Tool A"`
      - :json:`100`
      - :json:`"Well Stocked"`
    * - :json:`"Tool B"`
      - :json:`15`
      - :json:`"In Stock"`

Multiple Conditions
-------------------

Calculate discount based on price and category:

.. code-block:: sql

    SELECT name,
           price,
           CASE
               WHEN category = 'Electronics' AND price > 150 THEN price * 0.85
               WHEN category = 'Electronics' THEN price * 0.90
               WHEN price > 100 THEN price * 0.95
               ELSE price
           END AS discounted_price
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
      - :sql:`discounted_price`
    * - :json:`"Widget A"`
      - :json:`100`
      - :json:`90.0`
    * - :json:`"Widget B"`
      - :json:`150`
      - :json:`135.0`
    * - :json:`"Gadget X"`
      - :json:`200`
      - :json:`170.0`
    * - :json:`"Tool A"`
      - :json:`80`
      - :json:`80.0`
    * - :json:`"Tool B"`
      - :json:`120`
      - :json:`114.0`

Value Matching with CASE
-------------------------

Map category codes to names using WHEN conditions:

.. code-block:: sql

    SELECT name,
           category,
           CASE
               WHEN category = 'Electronics' THEN 'E'
               WHEN category = 'Hardware' THEN 'H'
               WHEN category = 'Media' THEN 'M'
               ELSE 'Other'
           END AS category_code
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`category`
      - :sql:`category_code`
    * - :json:`"Widget A"`
      - :json:`"Electronics"`
      - :json:`"E"`
    * - :json:`"Widget B"`
      - :json:`"Electronics"`
      - :json:`"E"`
    * - :json:`"Gadget X"`
      - :json:`"Electronics"`
      - :json:`"E"`
    * - :json:`"Tool A"`
      - :json:`"Hardware"`
      - :json:`"H"`
    * - :json:`"Tool B"`
      - :json:`"Hardware"`
      - :json:`"H"`

Nested CASE
-----------

CASE expressions can be nested:

.. code-block:: sql

    SELECT name,
           price,
           stock,
           CASE
               WHEN stock = 0 THEN 'Unavailable'
               ELSE CASE
                   WHEN price < 100 THEN 'Budget'
                   WHEN price < 150 THEN 'Standard'
                   ELSE 'Premium'
               END
           END AS product_tier
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`price`
      - :sql:`stock`
      - :sql:`product_tier`
    * - :json:`"Widget A"`
      - :json:`100`
      - :json:`50`
      - :json:`"Standard"`
    * - :json:`"Widget B"`
      - :json:`150`
      - :json:`5`
      - :json:`"Premium"`
    * - :json:`"Gadget X"`
      - :json:`200`
      - :json:`0`
      - :json:`"Unavailable"`
    * - :json:`"Tool A"`
      - :json:`80`
      - :json:`100`
      - :json:`"Budget"`
    * - :json:`"Tool B"`
      - :json:`120`
      - :json:`15`
      - :json:`"Standard"`

Important Notes
===============

Evaluation Order
----------------

CASE evaluates conditions **in order** and returns the first matching result. Once a match is found, remaining conditions are not evaluated:

.. code-block:: sql

    CASE
        WHEN x > 100 THEN 'High'      -- Evaluated first
        WHEN x > 50 THEN 'Medium'      -- Only checked if first is false
        WHEN x > 0 THEN 'Low'          -- Only checked if previous are false
        ELSE 'Zero or Negative'
    END

Order your conditions from most specific to least specific.

NULL Handling
-------------

NULL values in conditions are treated as FALSE:

.. code-block:: sql

    -- If price IS NULL, condition is FALSE
    CASE
        WHEN price > 100 THEN 'Expensive'
        ELSE 'Other'
    END

To explicitly check for NULL:

.. code-block:: sql

    CASE
        WHEN price IS NULL THEN 'No Price'
        WHEN price > 100 THEN 'Expensive'
        ELSE 'Affordable'
    END

Type Compatibility
------------------

.. important::

   All result expressions in a CASE statement must return compatible types.
   Mixing types (e.g., strings and numbers) will cause an error.

.. code-block:: sql

    -- VALID: All results are strings
    CASE
        WHEN x > 100 THEN 'High'
        WHEN x > 50 THEN 'Medium'
        ELSE 'Low'
    END

    -- INVALID: Mixed types (string and number)
    CASE
        WHEN x > 100 THEN 'High'
        ELSE 0
    END

See Also
========

* :ref:`Comparison Operators <comparison-operators>` - Operators used in CASE conditions
