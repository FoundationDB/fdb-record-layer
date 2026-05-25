===========
CREATE VIEW
===========

.. _create-view:

Clause in a :ref:`schema template definition <create-schema-template>` to create a non-materialized view.
A view is a virtual table whose contents are defined by a SQL query. Views do not store data themselves;
each query against a view executes the underlying query against the base tables.

Syntax
======

.. raw:: html
    :file: VIEW.diagram.svg

Parameters
==========

``viewName``
    The name of the view. Must be unique within the schema template — cannot collide with table, type,
    or other view names.

``query``
    The SQL ``SELECT`` statement that defines the view. The query can reference tables, other views,
    and functions defined in the same schema template, including those defined after the view
    (forward references are allowed).

Examples
========

Setup
-----

For these examples, assume the following schema template:

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

Views support joins, including self-joins:

.. code-block:: sql

    CREATE VIEW peer_pairs AS
        SELECT A.name AS emp1, B.name AS emp2
        FROM employees A, employees B
        WHERE A.dept = B.dept AND A.id < B.id

View with CTE
-------------

Views can use Common Table Expressions (CTEs):

.. code-block:: sql

    CREATE VIEW senior_engineering AS
        WITH filtered AS (
            SELECT id, name, salary
            FROM employees
            WHERE dept = 'Engineering' AND salary > 100000
        )
        SELECT * FROM filtered

Forward References
------------------

A view may reference tables or types that are defined later in the same schema template:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE my_template
        CREATE VIEW v AS SELECT * FROM orders
        CREATE TABLE orders (id BIGINT, amount BIGINT, PRIMARY KEY(id))

Indexes on Views
----------------

Indexes can be defined on views using the standard ``CREATE INDEX`` syntax.
This is particularly useful for pre-computing joins or array unnesting:

.. code-block:: sql

    CREATE TABLE products (
        id      BIGINT,
        tags    STRING ARRAY,
        PRIMARY KEY(id))

    CREATE VIEW product_tags AS
        SELECT id, tag FROM products AS p, p.tags AS tag

    CREATE INDEX idx_tags ON product_tags ORDER BY tag

See :doc:`INDEX` for full index syntax.

Limitations
===========

* **Non-materialized only**: Views are virtual — no data is stored. Each query re-executes the
  underlying query.
* **Read-only**: Views cannot be the target of ``INSERT``, ``UPDATE``, or ``DELETE`` statements.
* **No** :sql:`DROP VIEW`: Views are defined as part of a schema template. To remove a view,
  drop and recreate the schema template.
* **No prepared parameters**: View definitions cannot contain ``?`` placeholders.
* **Name uniqueness**: View names must not collide with table, type, or function names within
  the same schema template.
* **Functions referencing views**: A function defined in the same schema template cannot
  reference a view in its body if that view is also referenced in another view via a CTE
  (see `issue #3493 <https://github.com/FoundationDB/fdb-record-layer/issues/3493>`_).

See Also
========

* :ref:`create-schema-template` — Schema templates and their clauses
* :doc:`TABLE` — Defining tables within a schema template
* :doc:`INDEX` — Defining indexes, including indexes on views
* :doc:`FUNCTION` — User-defined functions
