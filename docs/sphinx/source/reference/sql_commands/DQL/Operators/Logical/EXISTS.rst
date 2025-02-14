======
EXISTS
======

Syntax
######

.. raw:: html
    :file: EXISTS.diagram.svg

Parameters
##########

* :sql:`subquery`
    A correlated subquery

Returns
#######

A Boolean that is :sql:`true` if and only if the the correlated subquery contains any row.

Examples
########

Existence of an element within an array
---------------------------------------

This supports queries with existential predicates that could be used, for example, to check whether a certain repeated
field matches certain criteria.

Let's assume the following table:

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

For example, suppose we want to return a list of all restaurants with *at least* one
review with a (good) rating, that is, larger or equal to 5. Since :sql:`reviews` is a repeated field in :sql:`RestaurantRecord`,
it is not possible to query it via a simple predicate. However, we can use an existential predicate to iterate over
its values using a correlated alias to the parent table and apply, for each review, the predicate on its :sql:`rating`:

.. code-block:: sql

    select * from restaurant as R where exists (select * from R.reviews where rating >= 5);

For the data above, this query will return:

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
    * - :json:`44`
      - :json:`"Restaurant3"`
      - .. code-block:: json

            [{"reviewer": "Reviewer41","rating": "3"},
            {"reviewer": "Reviewer55","rating": "5"}]

