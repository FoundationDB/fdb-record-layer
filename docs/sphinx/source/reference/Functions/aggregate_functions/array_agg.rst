=========
ARRAY_AGG
=========

.. _array-agg:

Collects the values of an expression across the rows of a group into a single array.

Syntax
======

.. raw:: html
    :file: array_agg.diagram.svg

Parameters
==========

``expression``
    The value collected from each row of the group. May be of any type except ``ARRAY``. The result is an array of the argument’s type.

``ALL``
    Collects every value of ``expression``, which is the default behavior when no set quantifier is present.

``IGNORE NULLS``
    Causes ``NULL`` values of ``expression`` to be omitted from the resulting array.

``RESPECT NULLS``
    Causes ``NULL`` values of ``expression`` to be collected as array elements. This is the default when no null-treatment clause is present. This behavior is subject to limitations; see the note on ``NULL`` handling under :ref:`Important Notes <array-agg-important-notes>`.

Returns
=======

Returns an array whose elements are the values of ``expression`` in the group. The order of elements within the array is unspecified.

The element type of the array is non-nullable when ``IGNORE NULLS`` is used, or when ``expression`` itself is non-nullable. Otherwise (that is, for a nullable ``expression`` with ``RESPECT NULLS`` behavior, which is the default) the element type is nullable. (However, see the note below regarding a current limitation on ``NULL`` elements in arrays.)

The behavior on empty input depends on whether a ``GROUP BY`` clause is present:

* Without ``GROUP BY``, aggregating over an empty input returns a single row whose array value is ``NULL``.
* With ``GROUP BY``, aggregating over an empty input returns no rows.

A group that does contain rows, but whose ``expression`` values are all ``NULL``, returns an empty array ``[]`` rather than ``NULL`` under ``IGNORE NULLS``. This holds whether or not a ``GROUP BY`` clause is present.

Examples
========

Setup
-----

For these examples, assume we have a ``sales`` table:

.. code-block:: sql

    CREATE TABLE sales (
        id BIGINT,
        product STRING,
        region STRING,
        amount BIGINT,
        PRIMARY KEY (id)
    )

    CREATE INDEX product_idx ON sales(product)

    INSERT INTO sales VALUES
        (1, 'Widget', 'North', 100),
        (2, 'Widget', 'South', 150),
        (3, 'Gadget', 'North', 200),
        (4, 'Gadget', 'South', NULL),
        (5, 'Widget', 'North', 120)

The ``product_idx`` index is needed for the ``GROUP BY product`` query to be planned; see the note on required indexes under :ref:`Important Notes <array-agg-important-notes>`.

ARRAY_AGG() without GROUP BY
----------------------------

The following query collects the amounts across the whole table into a single array. ``IGNORE NULLS`` is used here, so that ``NULL`` amounts are skipped rather than collected.

.. code-block:: sql

    SELECT ARRAY_AGG(amount IGNORE NULLS) AS amounts FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`amounts`
    * - :json:`[200, 100, 150, 120]`

Note that the ``NULL`` amount in row 4 is therefore omitted from the array.

Note also that the elements do not appear in ``id`` order. They are collected in whatever order the rows happen to be read in, and here the query is served by a scan of ``product_idx``, which visits the ``Gadget`` row before the ``Widget`` rows. Adding or removing an index may therefore change the order of the elements within the array.

ARRAY_AGG() with GROUP BY
-------------------------

The following query collects amounts per product.

.. code-block:: sql

    SELECT product, ARRAY_AGG(amount IGNORE NULLS) AS amounts
      FROM sales
     GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`amounts`
    * - :json:`"Gadget"`
      - :json:`[200]`
    * - :json:`"Widget"`
      - :json:`[100, 150, 120]`

The ``Gadget`` group contains two rows, but the ``NULL`` amount is omitted, so its array has a single element.

ARRAY_AGG() versus unnesting
----------------------------

Array aggregation can be viewed as the inverse operation of unnesting an array. The following example unnests an array literal into a stream of rows and then collects those rows back with ``ARRAY_AGG()``, reproducing the original array elements (although the order in which they come back is not guaranteed).

.. code-block:: sql

    SELECT ARRAY_AGG(x) AS numbers
      FROM (SELECT a FROM VALUES ([2, 1, -2, 3, -2, 1, 2]) AS T(a)) AS sq,
           sq.a AS x

.. list-table::
    :header-rows: 1

    * - :sql:`numbers`
    * - :json:`[2, 1, -2, 3, -2, 1, 2]`

See :ref:`Unnesting <unnesting>` for the unnesting syntax used by the inner query.

ARRAY_AGG() in a correlated subquery
------------------------------------

To collect a per-parent array of related child values, use a correlated subquery in the ``FROM`` clause. For the following example, assume a ``parent`` table and a ``child`` table joined on ``pid``:

.. code-block:: sql

    CREATE TABLE parent (pid BIGINT, name STRING, PRIMARY KEY (pid))

    CREATE TABLE child (cid BIGINT, pid BIGINT, val BIGINT, PRIMARY KEY (cid))

    CREATE INDEX child_by_pid ON child(pid)

    INSERT INTO parent VALUES (1, 'a'), (2, 'b'), (3, 'c')

    INSERT INTO child VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300), (4, 2, NULL)

The following query collects the ``val`` values of the children of each parent.

.. code-block:: sql

    SELECT p.pid, sq.vals
      FROM parent p,
           (SELECT ARRAY_AGG(c.val IGNORE NULLS) AS vals FROM child c WHERE c.pid = p.pid) sq

.. list-table::
    :header-rows: 1

    * - :sql:`pid`
      - :sql:`vals`
    * - :json:`1`
      - :json:`[100, 200]`
    * - :json:`2`
      - :json:`[300]`
    * - :json:`3`
      - :json:`null`

Parent 2 has two children, but the ``NULL`` value of the second one is omitted, so its array has a single element. Parent 3 has no matching child rows at all, so its array is ``NULL`` rather than empty.

.. _array-agg-important-notes:

Important notes
===============

* **Required indexes**: In general, ``GROUP BY`` queries require an appropriate index to be executed. See :ref:`Indexes <index_definition>` for details on creating indexes that support ``GROUP BY`` operations.
* **ARRAY_AGG() in indexes**: ``ARRAY_AGG()`` itself cannot currently be materialized in an index. Defining an index over it, as in ``CREATE INDEX idx AS SELECT ARRAY_AGG(val) FROM tab GROUP BY grp``, raises an ``UNSUPPORTED_OPERATION`` error.
* **Element order**: The order of the elements within the returned array is unspecified. Elements are collected in whatever order the rows are read in, which depends on the plan used to execute the query—in particular on which index is used, if any. You therefore cannot rely on the order. There is currently no way to request a particular order, since an in-call ``ORDER BY`` clause is not supported yet. This limitation is tracked by `Issue #4498 <https://github.com/FoundationDB/fdb-record-layer/issues/4498>`_.
* **NULL handling**: An array cannot currently hold ``NULL`` elements. This is due to a limitation at the level of the FDB Record Layer, tracked by `Issue #3646 <https://github.com/FoundationDB/fdb-record-layer/issues/3646>`_. A query that uses the default ``RESPECT NULLS`` behavior (including when no null-treatment clause is present) will fail at run time with an ``UNSUPPORTED_OPERATION`` error as soon as a ``NULL`` is encountered. To avoid this potential error, use ``IGNORE NULLS`` to omit ``NULL`` values from the array.
* **Arrays of arrays**: An ``ARRAY``-typed argument would produce an array of arrays, which is not supported. ``ARRAY_AGG()`` over an ``ARRAY`` column raises an ``UNSUPPORTED_OPERATION`` error. This limitation is tracked by `Issue #4167 <https://github.com/FoundationDB/fdb-record-layer/issues/4167>`_. To collect nested collections, you can wrap the inner array in a struct, as in ``ARRAY_AGG((rid, tags))``.
* **DISTINCT**: The ``DISTINCT`` set quantifier is not supported yet. The parser accepts ``ARRAY_AGG(DISTINCT «expression» …)`` but raises an ``UNSUPPORTED_QUERY`` error. This limitation is tracked by `Issue #4499 <https://github.com/FoundationDB/fdb-record-layer/issues/4499>`_.
* **Subqueries**: ``ARRAY_AGG()`` may be used in a correlated ``FROM``-clause subquery, as shown in `ARRAY_AGG() in a correlated subquery`_ above, but not in a scalar subquery in the ``SELECT`` projection list. The latter, for example ``SELECT p.pid, (SELECT ARRAY_AGG(c.val IGNORE NULLS) FROM child c WHERE c.pid = p.pid) FROM parent p``, raises a ``SYNTAX_ERROR``. That is a general limitation of scalar subqueries in projections, not specific to ``ARRAY_AGG()``.
