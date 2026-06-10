=========
Unnesting
=========

.. _unnesting:

Unnesting expands an array-valued field into a stream of rows, one per array element. The unnesting features in FDB Record Layer are aligned with the PartiQL notation, in which an array-typed reference appears as a table source in the ``FROM`` clause.

.. important::

   FDB Record Layer does *not* support the standard SQL ``UNNEST()`` table function.

Basic Syntax
============

To unnest an array field, reference it as a table source after the row source that contains it:

.. code-block:: sql

    SELECT table.*, element FROM table, table.array_column AS element

For each row of ``table``, the array is expanded and ``element`` is bound, in turn, to each element. The result is one row per input-row/element pair. If the array is ``NULL`` or empty in a row, that row contributes no output rows. In other words, the semantics of unnesting are comparable to an inner join.

.. important::

   FDB Record Layer does *not* support a ``LEFT OUTER JOIN`` variant for unnesting with outer join semantics.

As with other table sources, the ``AS`` keyword is optional:

.. code-block:: sql

    SELECT table.*, element FROM table, table.array_column element

If the array element type is a ``STRUCT``, ``element`` refers to the struct and its fields can be accessed via qualified names (``element.field``) or by star expansion (``element.*``). If the element type is a scalar, ``element`` refers directly to the scalar value and writing ``element.*`` is not allowed.

The query above shows the PartiQL notation for unnesting. An equivalent but more verbose way to express this unnesting operation is with a lateral subquery in the ``FROM`` clause:

.. code-block:: sql

    SELECT table.*, elements.element FROM table, (SELECT element FROM table.array_column) AS elements

The planner produces the same plan as with the PartiQL form.

Generating Ordinals with the ``AT`` Clause
------------------------------------------

In addition to the array elements themselves, unnesting can generate the 1-based position of each element in its original array. To do so, add an ``AT`` clause after the element alias:

.. code-block:: sql

    SELECT table.*, element, ordinal FROM table, table.array_column AS element AT ordinal

``ordinal`` is an ``INTEGER`` bound to the position of the element. The position is 1-based.

The element alias is optional when ``AT`` is used. In the rare case where only the ordinals are needed, you can leave it out:

.. code-block:: sql

    SELECT table.*, ordinal FROM table, table.array_column AT ordinal

The ``AT`` clause is only valid on an array-typed source. Applying it to a base table, view, or CTE raises a ``WRONG_OBJECT_TYPE`` error (SQLSTATE 42809).

.. note::

   Sorting on the ``AT`` ordinal is not supported.

Examples
========

Setup
-----

For these examples, assume we have the following type and table:

.. code-block:: sql

    CREATE TYPE AS STRUCT line_item (sku STRING, qty INTEGER)

    CREATE TABLE orders (
        order_id BIGINT,
        tags STRING ARRAY,
        items line_item ARRAY,
        prices DOUBLE ARRAY,
        PRIMARY KEY (order_id)
    )

    INSERT INTO orders VALUES
        (1, ['priority', 'gift'], [('A1', 2), ('A2', 1)],            [9.99, 19.99]),
        (2, ['backorder'],        [('B1', 3)],                       [4.50]),
        (3, NULL,                 [('C1', 1), ('C2', 2), ('C3', 1)], [29.99, 14.50, 5.00]),
        (4, [],                   [],                                [])

The ``prices`` array is positionally aligned with the ``items`` array: ``prices[i]`` is the unit price of ``items[i]``.

Unnesting a Scalar Array
------------------------

The following query unnests the ``tags`` array so that each tag becomes its own row:

.. code-block:: sql

    SELECT order_id, tag FROM orders, orders.tags AS tag

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`tag`
    * - :json:`1`
      - :json:`"priority"`
    * - :json:`1`
      - :json:`"gift"`
    * - :json:`2`
      - :json:`"backorder"`

Orders 3 and 4 contribute no rows because their ``tags`` value is ``NULL`` and ``[]``, respectively.

Unnesting a ``STRUCT`` Array
----------------------------

The following query unnests ``items`` and accesses the fields of the ``line_item`` struct via qualified names:

.. code-block:: sql

    SELECT order_id, item.sku, item.qty FROM orders, orders.items AS item

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`sku`
      - :sql:`qty`
    * - :json:`1`
      - :json:`"A1"`
      - :json:`2`
    * - :json:`1`
      - :json:`"A2"`
      - :json:`1`
    * - :json:`2`
      - :json:`"B1"`
      - :json:`3`
    * - :json:`3`
      - :json:`"C1"`
      - :json:`1`
    * - :json:`3`
      - :json:`"C2"`
      - :json:`2`
    * - :json:`3`
      - :json:`"C3"`
      - :json:`1`

Star Expansion on a ``STRUCT`` Element
--------------------------------------

Because each ``items`` element is a ``STRUCT``, ``item.*`` expands to one field per struct field:

.. code-block:: sql

    SELECT order_id, item.* FROM orders, orders.items AS item

This produces the same result as the previous example.

.. important::

   The ``alias.*`` notation is rejected when the array element type is a scalar (such as ``STRING`` or ``INTEGER``) or a ``VECTOR``, because there are no fields to expand. For instance, ``SELECT tag.* FROM orders, orders.tags AS tag`` is invalid.

   In the unusual case where you need a one-field record wrapping a scalar element, you can build one explicitly with a subquery:

   .. code-block:: sql

       SELECT order_id, sq.*
       FROM orders, (SELECT (tag) AS wrapped FROM orders.tags AS tag) AS sq

Unnesting in a Lateral Subquery
-------------------------------

The struct-array example above can equivalently be written with a lateral subquery:

.. code-block:: sql

    SELECT order_id, item.sku, item.qty FROM orders, (SELECT sku, qty FROM orders.items) AS item

The result is identical and the planner produces the same plan.

Element Ordinals with ``AT``
----------------------------

Use ``AT`` to query the 1-based ordinal of each element alongside its value:

.. code-block:: sql

    SELECT order_id, tag, at FROM orders, orders.tags AS tag AT at

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`tag`
      - :sql:`at`
    * - :json:`1`
      - :json:`"priority"`
      - :json:`1`
    * - :json:`1`
      - :json:`"gift"`
      - :json:`2`
    * - :json:`2`
      - :json:`"backorder"`
      - :json:`1`

If you only care about the positions, omit the element alias:

.. code-block:: sql

    SELECT order_id, at FROM orders, orders.items AT at

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`at`
    * - :json:`1`
      - :json:`1`
    * - :json:`1`
      - :json:`2`
    * - :json:`2`
      - :json:`1`
    * - :json:`3`
      - :json:`1`
    * - :json:`3`
      - :json:`2`
    * - :json:`3`
      - :json:`3`

This does not bind any alias to the elements themselves; only the ``at`` ordinal is in scope.

Ordinals and Filtering
----------------------

When you combine ``AT`` with a ``WHERE`` predicate to filter the elements, the elements keep the ordinals they had in the original array.

.. code-block:: sql

    SELECT order_id, item.sku, item.qty, at
      FROM orders, orders.items AS item AT at
     WHERE item.qty >= 2

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`sku`
      - :sql:`qty`
      - :sql:`at`
    * - :json:`1`
      - :json:`"A1"`
      - :json:`2`
      - :json:`1`
    * - :json:`2`
      - :json:`"B1"`
      - :json:`3`
      - :json:`1`
    * - :json:`3`
      - :json:`"C2"`
      - :json:`2`
      - :json:`2`

The row of Order 3 ``(C1, 1)`` is filtered out, but ``(C2, 2)`` keeps its original ordinal ``2``, not ``1``.

Unnesting Two Arrays in Parallel using ``AT`` as a Subscript
------------------------------------------------------------

When two arrays in the same row are positionally aligned, you can use the ``AT`` ordinal as a subscript for the second array to produce one row per element pair.

.. code-block:: sql

    SELECT order_id, item.sku, item.qty, orders.prices[at] AS price
    FROM orders, orders.items AS item AT at

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`sku`
      - :sql:`qty`
      - :sql:`price`
    * - :json:`1`
      - :json:`"A1"`
      - :json:`2`
      - :json:`9.99`
    * - :json:`1`
      - :json:`"A2"`
      - :json:`1`
      - :json:`19.99`
    * - :json:`2`
      - :json:`"B1"`
      - :json:`3`
      - :json:`4.50`
    * - :json:`3`
      - :json:`"C1"`
      - :json:`1`
      - :json:`29.99`
    * - :json:`3`
      - :json:`"C2"`
      - :json:`2`
      - :json:`14.50`
    * - :json:`3`
      - :json:`"C3"`
      - :json:`1`
      - :json:`5.00`

Computing the Cross Product of Two Unnested Arrays
--------------------------------------------------

Two array sources in ``FROM`` produce a cross product within each row. The ``AT`` ordinals on each side are independent.

.. code-block:: sql

    SELECT order_id, tag, item.sku, at_t, at_i
    FROM orders,
         orders.tags AS tag AT at_t,
         orders.items AS item AT at_i

For each order/tag pair, ``at_i`` restarts at ``1`` in the result:

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`tag`
      - :sql:`sku`
      - :sql:`at_t`
      - :sql:`at_i`
    * - :json:`1`
      - :json:`"priority"`
      - :json:`"A1"`
      - :json:`1`
      - :json:`1`
    * - :json:`1`
      - :json:`"priority"`
      - :json:`"A2"`
      - :json:`1`
      - :json:`2`
    * - :json:`1`
      - :json:`"gift"`
      - :json:`"A1"`
      - :json:`2`
      - :json:`1`
    * - :json:`1`
      - :json:`"gift"`
      - :json:`"A2"`
      - :json:`2`
      - :json:`2`
    * - :json:`2`
      - :json:`"backorder"`
      - :json:`"B1"`
      - :json:`1`
      - :json:`1`

Orders 3 and 4 produce no rows because at least one of their arrays is ``[]`` or ``NULL``.

``AT`` in a Correlated Subquery
-------------------------------

Both the element alias and the ``AT`` alias can be referenced from a later ``FROM`` item, including a lateral subquery that unnests another array:

.. code-block:: sql

    SELECT order_id, item.sku, sq.matched_price
      FROM orders,
           orders.items AS item AT at,
           (SELECT price AS matched_price
              FROM orders.prices AS price AT at2
             WHERE at2 = at AND price >= 15.0
           ) AS sq

For each item, the subquery looks up the price at the same ordinal and returns a row only if that price is at least ``15.0``:

.. list-table::
    :header-rows: 1

    * - :sql:`order_id`
      - :sql:`sku`
      - :sql:`matched_price`
    * - :json:`1`
      - :json:`"A2"`
      - :json:`19.99`
    * - :json:`3`
      - :json:`"C1"`
      - :json:`29.99`

See Also
========

* :doc:`Subqueries` - Subqueries and correlated subqueries
* :doc:`Joins` - Joining multiple tables
* :doc:`Indexes` - Indexing nested fields and unnested arrays
* :doc:`sql_commands/DQL/SELECT` - ``SELECT`` statement syntax
