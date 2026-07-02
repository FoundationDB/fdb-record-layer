======
SELECT
======

.. _select:

Syntax
######

.. raw:: html
    :file: SELECT.diagram.svg

Parameters
##########

* :sql:`selectExpression`
    This can be a column, a column inside a :sql:`STRUCT`, or an expression

* :sql:`label`
    Label to use for the projection expression. The label will be the only way to access this expression at higher scopes

Examples
########

To start, suppose we have the following table:

.. code-block:: sql

    CREATE TYPE AS STRUCT restaurant_review (reviewer STRING, rating INT64);
    CREATE TABLE restaurant (
        rest_no INT64,
        name STRING,
        reviews restaurant_review ARRAY,
        PRIMARY KEY(rest_no));

and the following records:

.. code-block:: json

    {"REST_NO": 42, "name": "Restaurant1", "reviews":
        [{"reviewer": "Reviewer11", "rating": 2},
        {"reviewer": "Reviewer22", "rating": 5},
        {"reviewer": "Reviewer21", "rating": 1}]}
    {"REST_NO": 43, "name": "Restaurant2", "reviews":
        [{"reviewer": "Reviewer19", "rating": 1}]}
    {"REST_NO": 44, "name": "Restaurant3", "reviews":
        [{"reviewer": "Reviewer41", "rating": 3},
        {"reviewer": "Reviewer55", "rating": 5}]}
    {"REST_NO": 45, "name": "Restaurant4", "reviews":
        [{"reviewer": "Reviewer14", "rating": 3},
        {"reviewer": "Reviewer55", "rating": 2}]}

Projecting all columns
----------------------

It is possible to query a table using normal :sql:`select` with optional predicates. For example:

.. code-block:: sql

    SELECT * FROM restaurant;

.. list-table::
    :header-rows: 1

    * - :sql:`rest_no`
      - :sql:`name`
      - :sql:`reviews`
    * - :json:`42`
      - :json:`"Restaurant1"`
      - .. code-block:: json

            [{"reviewer": "Reviewer11","rating": "2"},
            {"reviewer": "Reviewer22","rating": "5"},
            {"reviewer": "Reviewer21","rating": "1"}]
    * - :json:`43`
      - :json:`"Restaurant2"`
      - .. code-block:: json

            [{"reviewer": "Reviewer19","rating": "1"}]
    * - :json:`44`
      - :json:`"Restaurant3"`
      - .. code-block:: json

            [{"reviewer": "Reviewer41","rating": "3"},
            {"reviewer": "Reviewer55","rating": "5"}]
    * - :json:`45`
      - :json:`"Restaurant4"`
      - .. code-block:: json

            [{"reviewer": "Reviewer14","rating": "3"},
            {"reviewer": "Reviewer55","rating": "2"}]


Note how :sql:`*` resolves all the attributes (top-level fields) of table :sql:`restaurant` and returns them as individual columns in the
result set.

Projecting one column
---------------------

It is also possible to project individual columns from a table, for example, suppose we want to only project restaurant names:

.. code-block:: sql

    select name from restaurant

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Restaurant1"`
    * - :json:`"Restaurant2"`
    * - :json:`"Restaurant3"`
    * - :json:`"Restaurant4"`


Projecting the entire row as a struct
-------------------------------------

It is possible to project the entire row as a single :sql:`STRUCT` column rather than expanding individual fields. There are two equivalent ways to do this.

**Using** :sql:`(*)`

.. code-block:: sql

    SELECT (*) FROM restaurant;

This returns one column per row, named after the table, whose value is a :sql:`STRUCT` containing all of the table's fields:

.. list-table::
    :header-rows: 1

    * - :sql:`restaurant`
    * - :json:`{"REST_NO": 42, "NAME": "Restaurant1", "REVIEWS": [...]}`
    * - :json:`{"REST_NO": 43, "NAME": "Restaurant2", "REVIEWS": [...]}`
    * - :json:`{"REST_NO": 44, "NAME": "Restaurant3", "REVIEWS": [...]}`
    * - :json:`{"REST_NO": 45, "NAME": "Restaurant4", "REVIEWS": [...]}`

**Using the table name (or alias) as the projection expression**

As a shorthand, using the table name directly in the :sql:`SELECT` list is equivalent to :sql:`SELECT (*)`:

.. code-block:: sql

    SELECT restaurant FROM restaurant;

If the table has an alias, use the alias:

.. code-block:: sql

    SELECT r FROM restaurant r;

In both cases the result is identical to :sql:`SELECT (*) FROM restaurant`.

.. note::

    If a column in the table has the same name as the table itself, the column takes priority and its value is returned instead of the row struct.

Structs can be nested further by wrapping :sql:`(*)` in another level of parentheses with the :sql:`STRUCT` keyword:

.. code-block:: sql

    SELECT STRUCT ((*)) FROM restaurant;

This returns a single column whose value is a :sql:`STRUCT` containing one field, which is itself the row :sql:`STRUCT`.

.. note::
  The :sql:`STRUCT` keyword is required here since we are constructing a struct literal with a single field which is the row struct, see :ref:`struct_types` for more details.

Projecting column inside nested a nested :sql:`STRUCT`
------------------------------------------------------

It is also possible to project a nested field provided non of its parents is repeated:

.. code-block:: sql

    CREATE TYPE AS STRUCT A ( b B );
    CREATE TYPE AS STRUCT B ( c C );
    CREATE TYPE AS STRUCT C ( d D );
    CREATE TYPE AS STRUCT D ( e E );
    CREATE TYPE AS STRUCT E ( f int64 );
    CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id));
    SELECT a.b.c.d.e.f FROM tbl1;
