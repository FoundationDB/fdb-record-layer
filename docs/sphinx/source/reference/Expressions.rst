===========
Expressions
===========

This page provides an overview of all the expression forms supported by the Relational Layer.

Function calls
==============

Named functions are invoked using function-call syntax, such as ``COALESCE(a, b)``. They are documented in two sections:

- :doc:`Scalar functions <Functions>`, which operate on the values of a single row.
- :ref:`Aggregate functions <aggregate_functions>`, which aggregate values across multiple rows.

Arithmetic operators
====================

The following standard arithmetic operators are supported:

| ``+``  addition
| ``-``  subtraction
| ``*``  multiplication
| ``/``  division
| ``%``  modulo

These are all binary operators. If either of the two operands is ``NULL``, the result is ``NULL``.

.. _comparison-operators:

Comparison operators
====================

The following standard comparison operators are supported:

| ``<``  less than
| ``>``  greater than
| ``<=``  less than or equal to
| ``>=``  greater than or equal to
| ``=``  equal to
| ``!=``  not equal to
| ``<>``  not equal to (alias for ``!=``)

These are all binary operators. If either of the two operands is ``NULL``, the result is ``NULL``.

Some examples:

.. code-block:: sql

    SELECT * FROM products WHERE price > 100
    SELECT * FROM products WHERE price <= 50
    SELECT * FROM products WHERE name = 'Widget'
    SELECT * FROM products WHERE stock != 0

The following predicates extend equality comparison to handle ``NULL`` explicitly:

- :ref:`IS <is-operators>`
- :ref:`IS DISTINCT FROM <is-distinct-from>`

Unlike ``=`` and ``!=``, the ``IS DISTINCT FROM`` form treats ``NULL`` as a comparable value and never returns ``NULL``.

The Relational Layer also provides the following comparison predicates:

- :ref:`BETWEEN <between>`
- :ref:`LIKE <like>`
- :ref:`IN <in>`

Each of the predicates listed above has a negated ``NOT`` form (for example, ``NOT BETWEEN``).

Logical operators
=================

The following logical operators are supported:

| ``AND``  conjunction
| ``OR``  disjunction
| ``XOR``  exclusive or
| ``NOT``  negation

They follow the standard three-valued logic, in which an operand may be true, false, or ``NULL`` (unknown).

``OR``, ``AND``, and ``XOR`` are binary operators. The following table summarizes their results if either of the two operands is ``NULL``:

.. list-table::
    :header-rows: 1

    * - Left Operand
      - Right Operand
      - ``OR``
      - ``AND``
      - ``XOR``
    * - ``TRUE``
      - ``NULL``
      - ``TRUE``
      - ``NULL``
      - ``NULL``
    * - ``FALSE``
      - ``NULL``
      - ``NULL``
      - ``FALSE``
      - ``NULL``
    * - ``NULL``
      - ``TRUE``
      - ``TRUE``
      - ``NULL``
      - ``NULL``
    * - ``NULL``
      - ``FALSE``
      - ``NULL``
      - ``FALSE``
      - ``NULL``
    * - ``NULL``
      - ``NULL``
      - ``NULL``
      - ``NULL``
      - ``NULL``

The unary ``NOT`` operator inverts its operand:

.. list-table::
    :header-rows: 1

    * - Operand
      - ``NOT``
    * - ``TRUE``
      - ``FALSE``
    * - ``FALSE``
      - ``TRUE``
    * - ``NULL``
      - ``NULL``

Other expressions
=================

Several other expression forms are supported:

- :doc:`CASE <sql_commands/DQL/CASE>`, which returns a value based on the first of several conditions that evaluates to true.
- :doc:`CAST <Functions/cast_operator>`, which converts a value from one data type to another.
- :doc:`EXISTS <sql_commands/DQL/Operators/Logical/EXISTS>`, which tests whether a subquery returns any rows.

Detailed operator reference
===========================

The following pages provide detailed reference for some of the operators summarized above:

- :doc:`BETWEEN <sql_commands/DQL/Operators/BETWEEN>`
- :doc:`IN <sql_commands/DQL/Operators/IN>`
- :doc:`IS <sql_commands/DQL/Operators/IS>`
- :doc:`IS DISTINCT FROM <sql_commands/DQL/Operators/IS_DISTINCT_FROM>`
- :doc:`LIKE <sql_commands/DQL/Operators/LIKE>`

.. toctree::
    :hidden:
    :maxdepth: 1

    sql_commands/DQL/Operators/BETWEEN
    sql_commands/DQL/CASE
    Functions/cast_operator
    sql_commands/DQL/Operators/Logical/EXISTS
    sql_commands/DQL/Operators/IN
    sql_commands/DQL/Operators/IS
    sql_commands/DQL/Operators/IS_DISTINCT_FROM
    sql_commands/DQL/Operators/LIKE
