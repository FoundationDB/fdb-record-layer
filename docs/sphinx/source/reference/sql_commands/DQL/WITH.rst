#####
WITH
#####

A common table expression (CTE) defines a named query that can referenced multiple times within the scope of a SQL statement.
CTEs are visible within the query block in which they are defined, and they can nested.

Only non-recursive CTEs are supported at the moment.

Syntax
######

.. raw:: html
    :file: WITH.diagram.svg

Parameter
#########

* :sql:`cteName`
    The name of the common table expression.
* :sql:`columnAlias`
    The alias of the corresponding column of the CTE query.
* :sql:`cteQuery`
    The common table expression query.
* :sql:`query`
    The query the can optionally reference the CTE query via its given `cteName`.

Example
#######

Simple CTE
----------

Suppose we have the following tables representing a simple employee / department model:

.. code-block:: sql

    create table employees(id bigint, name string, dept bigint, primary key(id))
    insert into employees values (1, 'Dylan', 42),
                                 (2, 'Mark', 42),
                                 (3, 'Helly', 42),
                                 (4, 'Irving', 42),
                                 (5, 'Seth', 1),
                                 (50, 'Burt', 44)

Here is a simple CTE query:

.. code-block:: sql

    with macro_data_refinement_employees(name) as (select name from employees where dept = 42)
    select * from macro_data_refinement_employees

we get the following result:

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Dylan"`
    * - :json:`"Mark"`
    * - :json:`"Irving"`
    * - :json:`"Helly"`

Another CTE example
-------------------

Suppose we want to find out which restaurant has an average rating higher than 3, we can leverage CTE to break the query
into two logical parts, one part for calculating the restaurant names and their averages:

.. code-block:: sql

    select name, ar
    from restaurant as a, (select avg(rating) as ar from a.reviews) as x

The second part will operate on the result set of the first query, and filter out all the columns less or equal to 3, to
do this, we wrap the first part in a CTE and use it in the filtering query:

.. code-block:: sql

    with ratings(name, avgRating)
    as (select name, ar from restaurant as a, (select avg(rating) as ar from a.reviews) as x)
    select name, avgRating from ratings where avgRating > 3.0f

We get the desired result:

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`avgRating`
    * - :json:`"Restaurant44"`
      - :json:`4.0`
