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
