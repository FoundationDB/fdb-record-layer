========
GROUP BY
========

.. _group_by:

Groups rows that have the same values in specified columns into aggregated rows, typically used with aggregate functions.

Syntax
======

.. raw:: html
    :file: GROUP_BY.diagram.svg

The GROUP BY clause is used in SELECT statements:

.. code-block:: sql

    SELECT column1, aggregate_function(column2)
    FROM table_name
    GROUP BY column1

Parameters
==========

``GROUP BY expression [AS alias], ...``
    Groups rows based on the values of one or more expressions. Each unique combination of expression values creates a separate group.

``expression``
    Can be:

    - Column names
    - Nested field references (e.g., ``struct_column.field``)
    - Expressions or calculations

``alias`` (optional)
    An optional alias for the grouping expression

Returns
=======

Returns one row per unique combination of grouped values. When used with aggregate functions, computes aggregate values for each group.

Examples
========

Setup
-----

For these examples, assume we have an ``employees`` table:

.. code-block:: sql

    CREATE TABLE employees(
        id BIGINT,
        department STRING,
        role STRING,
        salary BIGINT,
        PRIMARY KEY(id))

    CREATE INDEX dept_idx AS SELECT department FROM employees ORDER BY department
    CREATE INDEX role_idx AS SELECT role FROM employees ORDER BY role

    INSERT INTO employees VALUES
        (1, 'Engineering', 'Developer', 100000),
        (2, 'Engineering', 'Developer', 110000),
        (3, 'Engineering', 'Manager', 150000),
        (4, 'Sales', 'Representative', 80000),
        (5, 'Sales', 'Manager', 120000)

GROUP BY Single Column
-----------------------

Count employees by department:

.. code-block:: sql

    SELECT department, COUNT(*) AS employee_count
    FROM employees
    GROUP BY department

.. list-table::
    :header-rows: 1

    * - :sql:`department`
      - :sql:`employee_count`
    * - :json:`"Engineering"`
      - :json:`3`
    * - :json:`"Sales"`
      - :json:`2`

GROUP BY Multiple Columns
--------------------------

Count employees by department and role:

.. code-block:: sql

    SELECT department, role, COUNT(*) AS employee_count
    FROM employees
    GROUP BY department, role

.. list-table::
    :header-rows: 1

    * - :sql:`department`
      - :sql:`role`
      - :sql:`employee_count`
    * - :json:`"Engineering"`
      - :json:`"Developer"`
      - :json:`2`
    * - :json:`"Engineering"`
      - :json:`"Manager"`
      - :json:`1`
    * - :json:`"Sales"`
      - :json:`"Representative"`
      - :json:`1`
    * - :json:`"Sales"`
      - :json:`"Manager"`
      - :json:`1`

GROUP BY with Aggregate Functions
-----------------------------------

Calculate average salary by department:

.. code-block:: sql

    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department

.. list-table::
    :header-rows: 1

    * - :sql:`department`
      - :sql:`avg_salary`
    * - :json:`"Engineering"`
      - :json:`120000.0`
    * - :json:`"Sales"`
      - :json:`100000.0`

Calculate multiple aggregates:

.. code-block:: sql

    SELECT department,
           COUNT(*) AS employee_count,
           MIN(salary) AS min_salary,
           MAX(salary) AS max_salary,
           AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department

.. list-table::
    :header-rows: 1

    * - :sql:`department`
      - :sql:`employee_count`
      - :sql:`min_salary`
      - :sql:`max_salary`
      - :sql:`avg_salary`
    * - :json:`"Engineering"`
      - :json:`3`
      - :json:`100000`
      - :json:`150000`
      - :json:`120000.0`
    * - :json:`"Sales"`
      - :json:`2`
      - :json:`80000`
      - :json:`120000`
      - :json:`100000.0`

.. _having:

GROUP BY with HAVING Clause
-----------------------------

Filter groups using HAVING:

.. code-block:: sql

    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
    HAVING AVG(salary) > 110000

.. list-table::
    :header-rows: 1

    * - :sql:`department`
      - :sql:`avg_salary`
    * - :json:`"Engineering"`
      - :json:`120000.0`

The HAVING clause filters groups after aggregation, unlike WHERE which filters rows before grouping.

GROUP BY with Column Aliases
------------------------------

Use aliases for grouped columns:

.. code-block:: sql

    SELECT department AS dept, COUNT(*) AS total
    FROM employees
    GROUP BY department AS dept

.. list-table::
    :header-rows: 1

    * - :sql:`dept`
      - :sql:`total`
    * - :json:`"Engineering"`
      - :json:`3`
    * - :json:`"Sales"`
      - :json:`2`

Important Notes
===============

Index Requirement
-----------------

**GROUP BY operations require an appropriate index for optimal performance.** The query planner needs an index on the grouped column(s) to execute the query efficiently. Without a suitable index, the query will fail with an "unable to plan" error.

Example index creation:

.. code-block:: sql

    CREATE INDEX dept_idx AS SELECT department FROM employees ORDER BY department

See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

Column Selection Rules
----------------------

* Only columns in the GROUP BY clause or aggregate functions can appear in the SELECT list
* Selecting non-grouped, non-aggregated columns will result in error 42803

**Invalid example**:

.. code-block:: sql

    -- ERROR: id is neither grouped nor aggregated
    SELECT id, department, COUNT(*)
    FROM employees
    GROUP BY department

**Valid example**:

.. code-block:: sql

    -- OK: all non-aggregated columns are grouped
    SELECT department, role, COUNT(*)
    FROM employees
    GROUP BY department, role

Nested Fields
-------------

GROUP BY supports grouping on nested struct fields:

.. code-block:: sql

    SELECT address.city, COUNT(*) AS resident_count
    FROM people
    GROUP BY address.city

Execution Model
---------------

FRL does not perform in-memory grouping. All GROUP BY operations must be backed by an appropriate index. This is a fundamental architectural constraint that ensures queries can execute efficiently over large datasets. An aggregate index will yield the best performance, but an index ordered by the desired grouping column will also work.

See Also
========

* :ref:`Aggregate Functions <aggregate_functions>` - Functions used with GROUP BY
* :ref:`Indexes <index_definition>` - Creating indexes for GROUP BY
* :ref:`SELECT Statement <select>` - Full SELECT syntax
