==========
Subqueries
==========

.. _subqueries:

A subquery is a query nested inside another query. Subqueries can appear in various parts of a SQL statement and can be correlated (referencing columns from outer queries) or non-correlated (independent of outer queries).

Subquery Types
==============

EXISTS Subqueries
-----------------

Tests whether a subquery returns any rows:

.. code-block:: sql

    SELECT columns FROM table WHERE EXISTS (subquery)

Subqueries in FROM Clause
--------------------------

A subquery can appear in the FROM clause as a derived table:

.. code-block:: sql

    SELECT columns FROM (subquery) AS alias

Correlated Subqueries
---------------------

A subquery that references columns from the outer query:

.. code-block:: sql

    SELECT columns FROM table1 AS t1
    WHERE EXISTS (SELECT * FROM table2 WHERE table2.col = t1.col)

Array Unnesting
---------------

FDB supports PartiQL-style array unnesting:

.. code-block:: sql

    SELECT columns FROM table, table.array_column AS alias

Examples
========

Setup
-----

For these examples, assume we have the following tables:

.. code-block:: sql

    CREATE TABLE a(ida INTEGER, x INTEGER, PRIMARY KEY(ida))
    CREATE TABLE x(idx INTEGER, y INTEGER, PRIMARY KEY(idx))
    CREATE TABLE b(idb INTEGER, q INTEGER, r INTEGER, PRIMARY KEY(idb))

    CREATE TYPE AS STRUCT s(f INTEGER)
    CREATE TABLE r(idr INTEGER, nr s ARRAY, PRIMARY KEY(idr))

    INSERT INTO a VALUES (1, 1), (2, 2), (3, 3)
    INSERT INTO x VALUES (4, 10), (5, 20), (6, 30)
    INSERT INTO b VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)
    INSERT INTO r VALUES
        (1, [(11), (12), (13)]),
        (2, [(21), (22), (23)]),
        (3, [(31), (32), (33)])

Non-Correlated EXISTS Subquery
-------------------------------

Check if a condition exists in a table, independent of the outer query:

.. code-block:: sql

    SELECT ida FROM a WHERE EXISTS (SELECT ida FROM a WHERE ida = 1)

.. list-table::
    :header-rows: 1

    * - :sql:`ida`
    * - :json:`1`
    * - :json:`2`
    * - :json:`3`

Since the subquery ``SELECT ida FROM a WHERE ida = 1`` returns at least one row (where ida = 1), the EXISTS condition is true for all rows in the outer query.

Correlated Subquery in FROM Clause
-----------------------------------

Use a correlated subquery as a derived table:

.. code-block:: sql

    SELECT x, sq.idr, sq.nr
    FROM a, (SELECT * FROM r WHERE r.idr = a.x) sq

.. list-table::
    :header-rows: 1

    * - :sql:`x`
      - :sql:`idr`
      - :sql:`nr`
    * - :json:`1`
      - :json:`1`
      - .. code-block:: json

            [{"f": 11}, {"f": 12}, {"f": 13}]
    * - :json:`2`
      - :json:`2`
      - .. code-block:: json

            [{"f": 21}, {"f": 22}, {"f": 23}]
    * - :json:`3`
      - :json:`3`
      - .. code-block:: json

            [{"f": 31}, {"f": 32}, {"f": 33}]

The subquery ``(SELECT * FROM r WHERE r.idr = a.x)`` is correlated because it references ``a.x`` from the outer query.

Array Unnesting with PartiQL
-----------------------------

Unnest an array column using PartiQL syntax:

.. code-block:: sql

    SELECT idr FROM r, r.nr AS NEST WHERE NEST.f = 23

.. list-table::
    :header-rows: 1

    * - :sql:`idr`
    * - :json:`2`

This query iterates over the ``nr`` array in each row of ``r`` and returns rows where the nested struct's ``f`` field equals 23.

You can also use a subquery for array unnesting:

.. code-block:: sql

    SELECT idr FROM r, (SELECT * FROM r.nr) AS NEST WHERE NEST.f = 23

.. list-table::
    :header-rows: 1

    * - :sql:`idr`
    * - :json:`2`

Correlated Subquery with GROUP BY
----------------------------------

Use a correlated subquery with aggregation:

.. code-block:: sql

    SELECT x FROM a
    WHERE EXISTS (
        SELECT a.x, MAX(idb) FROM b
        WHERE q > a.x
        GROUP BY q
    )

.. list-table::
    :header-rows: 1

    * - :sql:`x`
    * - :json:`1`
    * - :json:`2`
    * - :json:`3`

The subquery references ``a.x`` from the outer query in both the SELECT list and WHERE clause. For each row in ``a``, the subquery finds rows in ``b`` where ``q`` is greater than the current ``a.x`` value and groups them by ``q``.

Correlation in Aggregation Functions
-------------------------------------

Correlations can also appear inside aggregate functions:

.. code-block:: sql

    SELECT x FROM a
    WHERE EXISTS (
        SELECT MAX(a.x), MAX(idb) FROM b
        WHERE q > x
        GROUP BY q
    )

.. list-table::
    :header-rows: 1

    * - :sql:`x`
    * - :json:`1`
    * - :json:`2`
    * - :json:`3`

Here, ``MAX(a.x)`` aggregates the correlated column ``a.x``, which has the same value for all rows processed by each instance of the subquery.

Important Notes
===============

Non-Qualified Column References
--------------------------------

When a column reference is not qualified with a table alias, the query planner first tries to resolve it in the current query block. If not found, it looks in the outer query blocks:

.. code-block:: sql

    -- 'x' refers to column x from table a, not table x
    SELECT idx FROM x WHERE EXISTS (SELECT x FROM a WHERE ida = 1)

Subquery Scope
--------------

Subqueries in the FROM clause create a new query scope. Columns from the subquery are accessed via the subquery alias:

.. code-block:: sql

    SELECT sq.column FROM (SELECT column FROM table) AS sq

See Also
========

* :doc:`sql_commands/DQL/Operators/Logical/EXISTS` - EXISTS operator documentation
* :doc:`sql_commands/DQL/WHERE` - WHERE clause filtering
* :doc:`sql_commands/DQL/SELECT` - SELECT statement syntax

