===========
CREATE VIEW
===========

.. _create-view:

Clause in a :ref:`schema template definition <create-schema-template>` to create a non-materialized view. A view is a virtual table whose contents are defined by a SQL query. Views do not store data themselves; each query against a view executes the underlying query against the base tables. Views are read-only and cannot be the target of ``INSERT``, ``UPDATE``, or ``DELETE`` statements. ``CREATE VIEW`` is a clause within a schema template and cannot be a standalone statement. View names must not collide with table, type, or function names in the same schema template.

Syntax
======

.. raw:: html
    :file: VIEW.diagram.svg

Parameters
==========

``viewName``
    The name of the view. Must be unique within the schema template — cannot collide with table, type, or other view names.

``query``
    The SQL ``SELECT`` statement that defines the view. The query can reference tables, other views, and functions defined in the same schema template.

Examples
========

Setup
-----

``CREATE VIEW`` must appear inside a :ref:`schema template <create-schema-template>` definition and cannot be a standalone statement. For the examples on this page, assume the following schema template:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE my_template
        CREATE TABLE employees (
            id       BIGINT,
            name     STRING,
            dept     STRING,
            salary   BIGINT,
            PRIMARY KEY(id))

Basic View
----------

Create a view that filters rows from a base table:

.. code-block:: sql

    CREATE VIEW engineering AS
        SELECT id, name, salary
        FROM employees
        WHERE dept = 'Engineering'

Query the view like a table:

.. code-block:: sql

    SELECT * FROM engineering

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`salary`
    * - :json:`1`
      - :json:`"Alice"`
      - :json:`100000`
    * - :json:`2`
      - :json:`"Bob"`
      - :json:`110000`

Nested Views
------------

Views can reference other views. The following creates a second view on top of the first:

.. code-block:: sql

    CREATE VIEW engineering AS
        SELECT id, name, salary
        FROM employees
        WHERE dept = 'Engineering'

    CREATE VIEW high_earners AS
        SELECT id, name
        FROM engineering
        WHERE salary > 100000

.. code-block:: sql

    SELECT * FROM high_earners

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
    * - :json:`2`
      - :json:`"Bob"`

View with JOIN
--------------

Views support joins, including self-joins (see :ref:`INNER JOIN <inner_join>` for join syntax):

.. code-block:: sql

    CREATE VIEW peer_pairs AS
        SELECT A.name AS emp1, B.name AS emp2
        FROM employees A, employees B
        WHERE A.dept = B.dept AND A.id < B.id

View with CTE
-------------

Views can use Common Table Expressions (see :doc:`WITH <../../DQL/WITH>` for CTE syntax):

.. code-block:: sql

    CREATE VIEW senior_engineering AS
        WITH filtered AS (
            SELECT id, name, salary
            FROM employees
            WHERE dept = 'Engineering' AND salary > 100000
        )
        SELECT * FROM filtered

Indexes on Views
----------------

Indexes can be defined on views using the standard ``CREATE INDEX`` syntax. For a self-contained example including array unnesting, see :ref:`index-on-syntax` in :doc:`INDEX`.

See Also
========

* :ref:`create-schema-template` — Schema templates and their clauses
* :doc:`TABLE` — Defining tables within a schema template
* :doc:`INDEX` — Defining indexes, including :ref:`index-on-syntax` for indexes on views
* :doc:`FUNCTION` — User-defined functions
