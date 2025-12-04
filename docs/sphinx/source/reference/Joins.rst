=====
Joins
=====

.. _joins:

Joins combine rows from two or more tables based on a related column. FDB Record Layer supports joins using comma-separated table references in the FROM clause with join conditions specified in the WHERE clause.

.. important::

   FDB Record Layer **does not support** standard SQL JOIN keywords (``INNER JOIN``, ``LEFT JOIN``, ``RIGHT JOIN``, ``OUTER JOIN``, etc.). Use comma-separated FROM clause instead.

Basic Join Syntax
=================

Cross Join (Cartesian Product)
-------------------------------

List multiple tables separated by commas:

.. code-block:: sql

    SELECT columns FROM table1, table2

This produces a Cartesian product of all rows from both tables.

Equijoin with WHERE Clause
---------------------------

Use WHERE clause to specify join conditions:

.. code-block:: sql

    SELECT columns
    FROM table1, table2
    WHERE table1.column = table2.column

This is equivalent to an INNER JOIN in standard SQL.

Examples
========

Setup
-----

For these examples, assume we have the following tables:

.. code-block:: sql

    CREATE TABLE emp(
        id BIGINT,
        fname STRING,
        lname STRING,
        dept_id BIGINT,
        PRIMARY KEY(id)
    )

    CREATE TABLE dept(
        id BIGINT,
        name STRING,
        PRIMARY KEY(id)
    )

    CREATE TABLE project(
        id BIGINT,
        name STRING,
        dsc STRING,
        emp_id BIGINT,
        PRIMARY KEY(id)
    )

    INSERT INTO emp VALUES
        (1, 'Jack', 'Williams', 1),
        (2, 'Thomas', 'Johnson', 1),
        (3, 'Emily', 'Martinez', 1),
        (5, 'Daniel', 'Miller', 2),
        (8, 'Megan', 'Miller', 3)

    INSERT INTO dept VALUES
        (1, 'Engineering'),
        (2, 'Sales'),
        (3, 'Marketing')

    INSERT INTO project VALUES
        (1, 'OLAP', 'Support OLAP queries', 3),
        (2, 'SEO', 'Increase visibility on search engines', 8),
        (3, 'Feedback', 'Turn customer feedback into items', 5)

Simple Two-Table Join
----------------------

Join employees with their departments:

.. code-block:: sql

    SELECT fname, lname
    FROM emp, dept
    WHERE emp.dept_id = dept.id
      AND dept.name = 'Engineering'

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
    * - :json:`"Jack"`
      - :json:`"Williams"`
    * - :json:`"Thomas"`
      - :json:`"Johnson"`
    * - :json:`"Emily"`
      - :json:`"Martinez"`

Three-Way Join
--------------

Join across three tables to find departments and their projects:

.. code-block:: sql

    SELECT dept.name, project.name
    FROM emp, dept, project
    WHERE emp.dept_id = dept.id
      AND project.emp_id = emp.id

.. list-table::
    :header-rows: 1

    * - :sql:`dept.name`
      - :sql:`project.name`
    * - :json:`"Engineering"`
      - :json:`"OLAP"`
    * - :json:`"Sales"`
      - :json:`"Feedback"`
    * - :json:`"Marketing"`
      - :json:`"SEO"`

The join conditions create relationships: employees are linked to departments via ``dept_id``, and projects are linked to employees via ``emp_id``.

Join with Subquery
------------------

Use a derived table (subquery) in a join:

.. code-block:: sql

    SELECT fname, lname
    FROM (
        SELECT fname, lname, dept_id
        FROM emp
        WHERE EXISTS (SELECT * FROM project WHERE emp_id = emp.id)
    ) AS sq, dept
    WHERE sq.dept_id = dept.id
      AND dept.name = 'Sales'

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
    * - :json:`"Daniel"`
      - :json:`"Miller"`

This finds employees who are assigned to projects and work in the Sales department.

Nested Joins
------------

Join subqueries that themselves contain joins:

.. code-block:: sql

    SELECT sq.name, project.name
    FROM (
        SELECT dept.name, emp.id
        FROM emp, dept
        WHERE emp.dept_id = dept.id
    ) AS sq, project
    WHERE project.emp_id = sq.id

.. list-table::
    :header-rows: 1

    * - :sql:`sq.name`
      - :sql:`project.name`
    * - :json:`"Engineering"`
      - :json:`"OLAP"`
    * - :json:`"Sales"`
      - :json:`"Feedback"`
    * - :json:`"Marketing"`
      - :json:`"SEO"`

The subquery first joins employees with departments, then the result is joined with projects.

Join with CTEs
--------------

Use Common Table Expressions in joins:

.. code-block:: sql

    WITH c1(w, z) AS (SELECT id, col1 FROM t1),
         c2(a, b) AS (SELECT id, col1 FROM t1 WHERE id IN (1, 2))
    SELECT * FROM c1, c2

This creates two CTEs and joins them using a cross join.

Self-Join
---------

Join a table to itself:

.. code-block:: sql

    SELECT * FROM Table1, Table1 WHERE col1 = 10

This self-join can be used to find relationships within the same table. Use aliases to distinguish between the two references:

.. code-block:: sql

    SELECT t1.fname, t2.fname
    FROM emp t1, emp t2
    WHERE t1.dept_id = t2.dept_id
      AND t1.id < t2.id

Semi-Join with EXISTS
----------------------

Use EXISTS to implement a semi-join (find rows that have matching rows in another table):

.. code-block:: sql

    SELECT fname, lname
    FROM emp
    WHERE EXISTS (
        SELECT * FROM project WHERE emp_id = emp.id
    )

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
    * - :json:`"Emily"`
      - :json:`"Martinez"`
    * - :json:`"Daniel"`
      - :json:`"Miller"`
    * - :json:`"Megan"`
      - :json:`"Miller"`

This finds all employees who have at least one project assigned, without returning duplicate employee rows.

Join with User-Defined Functions
---------------------------------

Join results from user-defined functions:

.. code-block:: sql

    SELECT A.col1, A.col2, B.col1, B.col2
    FROM f1(103, 'b') A, f1(103, 'b') B
    WHERE A.col1 = B.col1

User-defined functions can be used like tables in the FROM clause and joined with join conditions in the WHERE clause.

Important Notes
===============

Table Aliases
-------------

Use aliases to:

- Distinguish between multiple references to the same table
- Shorten long table names
- Reference columns from specific tables in multi-table joins

.. code-block:: sql

    SELECT e.fname, d.name
    FROM emp e, dept d
    WHERE e.dept_id = d.id

Join Conditions
---------------

- Join conditions should be specified in the WHERE clause
- Use ``AND`` to combine multiple join conditions and filters
- Missing join conditions result in a Cartesian product (all combinations)

See Also
========

* :doc:`sql_commands/DQL/SELECT` - SELECT statement syntax
* :doc:`sql_commands/DQL/WHERE` - WHERE clause filtering
* :doc:`Subqueries` - Subqueries and correlated subqueries
* :doc:`sql_commands/DQL/WITH` - Common Table Expressions

