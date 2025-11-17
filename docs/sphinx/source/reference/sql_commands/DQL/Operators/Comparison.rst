====================
Comparison Operators
====================

.. _comparison-operators:

Standard Comparison Operators
==============================

We support standard comparison operators: ``>``, ``>=``, ``=``, ``<``, ``<=``, ``!=``, ``<>``

If either the left or right operand is NULL, the comparison result is NULL.

.. code-block:: sql

    SELECT * FROM products WHERE price > 100
    SELECT * FROM products WHERE price <= 50
    SELECT * FROM products WHERE name = 'Widget'
    SELECT * FROM products WHERE stock != 0

IS DISTINCT FROM
================

.. _is_distinct_from:

Syntax
------

.. code-block:: sql

    expression1 IS [NOT] DISTINCT FROM expression2

NULL-safe equality comparison that treats NULL values as equal to each other.

Behavior
--------

``IS DISTINCT FROM`` returns:

- ``FALSE`` if both values are NULL
- ``FALSE`` if both values are equal (and non-NULL)
- ``TRUE`` otherwise

``IS NOT DISTINCT FROM`` returns:

- ``TRUE`` if both values are NULL
- ``TRUE`` if both values are equal (and non-NULL)
- ``FALSE`` otherwise

Comparison with ``=``
---------------------

Unlike the standard ``=`` operator, ``IS DISTINCT FROM`` never returns NULL:

.. list-table::
    :header-rows: 1

    * - Expression
      - Result
    * - ``NULL = NULL``
      - ``NULL``
    * - ``NULL IS DISTINCT FROM NULL``
      - ``FALSE``
    * - ``5 = NULL``
      - ``NULL``
    * - ``5 IS DISTINCT FROM NULL``
      - ``TRUE``
    * - ``5 = 5``
      - ``TRUE``
    * - ``5 IS DISTINCT FROM 5``
      - ``FALSE``

Examples
--------

.. code-block:: sql

    -- Find rows where value is different from 100 (including NULLs)
    SELECT * FROM products
    WHERE price IS DISTINCT FROM 100

    -- Find rows where value equals 100 or is NULL
    SELECT * FROM products
    WHERE price IS NOT DISTINCT FROM 100

    -- NULL-safe equality check
    SELECT * FROM products
    WHERE price IS NOT DISTINCT FROM NULL  -- Finds rows where price IS NULL

Use Cases
---------

``IS DISTINCT FROM`` is particularly useful for:

- Detecting changes between values when NULLs are involved
- Implementing NULL-safe equality checks
- Comparing values where NULL is a meaningful state

See Also
========

* :ref:`IS Operator <is-operators>` - IS NULL, IS TRUE, IS FALSE
