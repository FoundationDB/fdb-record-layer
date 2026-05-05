====
JOIN
====

.. _inner_join:
.. _left_outer_join:

The FDB Record Layer supports ``INNER JOIN`` and ``LEFT OUTER JOIN``. Both accept an ``ON`` condition or a ``USING`` column list. The ``RIGHT OUTER JOIN`` and ``FULL OUTER JOIN`` variants are not yet supported.

INNER JOIN
==========

An ``INNER JOIN`` combines rows from two sources based on a join condition. It returns only the rows where the join condition is satisfied in both tables.

Syntax
------

.. raw:: html
    :file: INNER_JOIN.diagram.svg

.. code-block:: sql

    SELECT columns
    FROM table1 INNER JOIN table2
    ON table1.column = table2.column

The ``INNER`` keyword is optional: ``JOIN`` can be used as a shorthand for ``INNER JOIN``.

Parameters
----------

``source``
    The table or subquery to join with.

``ON joinCondition``
    A boolean expression that specifies the join condition. Only rows where this condition evaluates to true are included in the result.

``USING (column(s))``
    A comma-separated list of column names that exist in both tables. This is shorthand for joining on equality of these columns. The ``USING`` clause automatically removes duplicate columns from the result set.

Returns
-------

Returns a result set containing rows where the join condition is satisfied. For each matching pair of rows from the two sources, a combined row is produced with columns from both tables.

LEFT OUTER JOIN
===============

Combines rows from two sources based on a join condition. Unlike an inner join, a ``LEFT OUTER JOIN`` preserves every row from the left (preserved) side. When no matching row exists on the right (null-supplying) side, the right-side columns are filled with ``NULL``.

Syntax
------

.. code-block:: sql

    SELECT columns
    FROM table1 LEFT JOIN table2
    ON table1.column = table2.column

The ``OUTER`` keyword is optional: ``LEFT JOIN`` and ``LEFT OUTER JOIN`` are equivalent.

Parameters
----------

``source``
    The table or subquery to join with.

``ON joinCondition``
    A boolean expression that specifies the join condition. Rows from the left side that have no match on the right produce a single output row with ``NULL`` in every right-side column.

``USING (column(s))``
    A comma-separated list of column names that exist in both tables. This is shorthand for joining on equality of these columns. As with ``INNER JOIN … USING``, duplicate columns are hidden from ``SELECT *`` expansion.

Returns
-------

Returns a result set containing:

- One combined row for every matching pair (same as an inner join).
- One null-padded row for every left-side row that has *no* match on the right side.

Every left-side row therefore appears at least once in the output.

Examples
========

For the examples below, assume the following tables:

.. code-block:: sql

    CREATE TABLE emp (
        id BIGINT, fname STRING, lname STRING, dept_id BIGINT,
        PRIMARY KEY(id)
    )
    CREATE TABLE dept (
        id BIGINT, name STRING,
        PRIMARY KEY(id)
    )

    INSERT INTO emp VALUES
        (1, 'Alice', 'Smith', 1),
        (2, 'Bob',   'Jones', 1),
        (3, 'Carol', 'Lee',   2),
        (4, 'Dave',  'Kim',  99)

    INSERT INTO dept VALUES
        (1, 'Engineering'),
        (2, 'Sales'),
        (3, 'Marketing')

Basic LEFT JOIN
---------------

Return every employee together with their department. Dave (``dept_id = 99``) has no matching department and receives ``NULL``:

.. code-block:: sql

    SELECT e.fname, e.lname, d.name
    FROM emp e LEFT JOIN dept d ON e.dept_id = d.id

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
      - :sql:`name`
    * - :json:`"Alice"`
      - :json:`"Smith"`
      - :json:`"Engineering"`
    * - :json:`"Bob"`
      - :json:`"Jones"`
      - :json:`"Engineering"`
    * - :json:`"Carol"`
      - :json:`"Lee"`
      - :json:`"Sales"`
    * - :json:`"Dave"`
      - :json:`"Kim"`
      - ``NULL``

Anti-Join Pattern
-----------------

Use ``LEFT JOIN`` with ``WHERE … IS NULL`` on the right side to find rows with *no* match — the standard SQL anti-join pattern:

.. code-block:: sql

    SELECT e.fname, e.lname
    FROM emp e LEFT JOIN dept d ON e.dept_id = d.id
    WHERE d.id IS NULL

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
    * - :json:`"Dave"`
      - :json:`"Kim"`

Chained Joins
-------------

A ``LEFT JOIN`` can follow an ``INNER JOIN`` or another ``LEFT JOIN``:

.. code-block:: sql

    SELECT e.fname, d.name, p.name
    FROM emp e
    JOIN dept d ON e.dept_id = d.id
    LEFT JOIN project p ON e.id = p.emp_id

For more examples (consecutive joins, subquery joins, self-joins, CTEs), see :doc:`../../Joins`.

See Also
========

* :doc:`SELECT` - SELECT statement syntax
* :doc:`WHERE` - WHERE clause filtering
* :doc:`../../Subqueries` - Subqueries and correlated subqueries
* :doc:`WITH` - Common Table Expressions
* :doc:`../../Joins` - General overview of joins, with examples
