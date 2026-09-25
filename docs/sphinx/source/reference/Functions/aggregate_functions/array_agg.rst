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

``ORDER BY sort expression [ASC|DESC] [NULLS FIRST|NULLS LAST] [, …]``
    Orders the elements within the resulting array. See :ref:`ARRAY_AGG() with ORDER BY <array-agg-ordering>` below for the conditions under which an ordering can be evaluated.

``LIMIT count``
    Collects at most ``count`` elements, discarding any further values of the group. ``count`` must be a non-negative integer literal. A limit of ``0`` yields an empty array.

Returns
=======

Returns an array whose elements are the values of ``expression`` in the group. The elements are ordered as specified by the in-call ``ORDER BY`` clause; absent such a clause, their order is unspecified and must not be relied on. See :ref:`ARRAY_AGG() with ORDER BY <array-agg-ordering>`.

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

Note also that the elements do not appear in ``id`` order. They are collected in whatever order the rows happen to be read in, which depends on the plan chosen to execute the query, in particular on which index that plan uses, if any. Here the query is served by a scan of ``product_idx``, which visits the ``Gadget`` row before the ``Widget`` rows. Adding or removing an index may therefore change the order of the elements within the array, so you cannot rely on it. To request a particular order, use an in-call ``ORDER BY`` clause; see :ref:`ARRAY_AGG() with ORDER BY <array-agg-ordering>`.

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

.. _array-agg-ordering:

ARRAY_AGG() with ORDER BY
-------------------------

An in-call ``ORDER BY`` clause orders the elements within each array.

.. note::

    The query planner cannot introduce a sort of its own to satisfy this clause. In order to evaluate it, the planner needs to be able to find a query plan that enumerates the rows in the required order; you will therefore usually have to define a suitable index that provides the order. When no suitable access path exists, the query raises an ``UNSUPPORTED_QUERY`` error.

Without a ``GROUP BY`` clause there is only one group, so the ordering has to be provided on the sort keys alone. For a grouped aggregation the index has to be ordered by the grouping key followed by the sort keys. The aggregated expression itself does not have to appear in the index, as the record fetch preserves the order. The following example query therefore requires an additional index to be defined:

.. code-block:: sql

    CREATE INDEX product_amount_idx ON sales(product, amount)

With that index in place, the following query collects each product's amounts from largest to smallest.

.. code-block:: sql

    SELECT product, ARRAY_AGG(amount IGNORE NULLS ORDER BY amount DESC) AS amounts
      FROM sales
     GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`amounts`
    * - :json:`"Widget"`
      - :json:`[150, 120, 100]`
    * - :json:`"Gadget"`
      - :json:`[200]`

A few points are worth keeping in mind:

* The sort keys have to follow all of the grouping key parts, since only they order the rows *within* a group. The grouping key parts are requested in the order in which they are written, so an index whose own order permutes them will not be used even though it would group the rows just as well.
* All the sort keys have to share one direction. A single ``DESC`` is fine, as it can be served by a reverse scan of the same index, but a mixture such as ``ORDER BY a ASC, b DESC`` generally cannot be evaluated, because one scan of one index provides either the index order or its exact reverse.
* The default null placement is the one the underlying storage order already provides, namely ``NULLS FIRST`` for an ascending sort key and ``NULLS LAST`` for a descending one. Spelling either of those out changes nothing, whereas the opposite placement, ``ASC NULLS LAST`` or ``DESC NULLS FIRST``, generally cannot be evaluated.
* Several ``ARRAY_AGG()`` aggregates may share the same ordering. Aggregates without an in-call ``ORDER BY`` clause can be added as well, as they impose no ordering of their own. Two aggregates whose ``ORDER BY`` clauses differ, however, raise an ``UNSUPPORTED_QUERY`` error. Note that this is a restriction of the current implementation rather than a fundamental one: the clauses are compared as written, so even ``ORDER BY x`` alongside ``ORDER BY x, y`` is rejected, although a single stream ordered by ``x, y`` would serve both.
* An ``ORDER BY`` clause can only refer to the input of its own aggregate. One that refers to an enclosing query, as in a correlated subquery, raises an ``UNSUPPORTED_QUERY`` error.

ARRAY_AGG() with LIMIT
----------------------

The following query collects at most two amounts, discarding the rest of the group.

.. code-block:: sql

    SELECT ARRAY_AGG(amount IGNORE NULLS LIMIT 2) AS amounts FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`amounts`
    * - :json:`[200, 100]`

Which two amounts are retained follows from the order in which the query plan happens to read the rows. To select a particular pair, combine ``LIMIT`` with an in-call ``ORDER BY`` clause (see :ref:`ARRAY_AGG() with ORDER BY <array-agg-ordering>`). This way you can turn the aggregation into a top-n list, as in the following query, which collects the two largest amounts per product:

.. code-block:: sql

    SELECT product, ARRAY_AGG(amount IGNORE NULLS ORDER BY amount DESC LIMIT 2) AS amounts
      FROM sales
     GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`amounts`
    * - :json:`"Widget"`
      - :json:`[150, 120]`
    * - :json:`"Gadget"`
      - :json:`[200]`

Besides producing top-n lists, a ``LIMIT`` clause can also be useful to impose a bound on the cost of the aggregation; see the note on group size under :ref:`Important Notes <array-agg-important-notes>`.

ARRAY_AGG() in a correlated subquery
------------------------------------

To collect a per-parent array of related child values, use a correlated subquery in the ``FROM`` clause. For the following example, assume a ``parent`` table and a ``child`` table joined on ``pid``:

.. code-block:: sql

    CREATE TABLE parent (pid BIGINT, name STRING, PRIMARY KEY (pid))

    CREATE TABLE child (cid BIGINT, pid BIGINT, val BIGINT, PRIMARY KEY (cid))

    CREATE INDEX child_by_pid ON child(pid)

    CREATE INDEX child_by_pid_val ON child(pid, val)

    INSERT INTO parent VALUES (1, 'a'), (2, 'b'), (3, 'c')

    INSERT INTO child VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300), (4, 2, NULL)

The following query collects the ``val`` values of the children of each parent, largest first.

.. code-block:: sql

    SELECT p.pid, sq.vals
      FROM parent p,
           (SELECT ARRAY_AGG(c.val IGNORE NULLS ORDER BY c.val DESC) AS vals FROM child c WHERE c.pid = p.pid) sq

.. list-table::
    :header-rows: 1

    * - :sql:`pid`
      - :sql:`vals`
    * - :json:`1`
      - :json:`[200, 100]`
    * - :json:`2`
      - :json:`[300]`
    * - :json:`3`
      - :json:`null`

Parent 2 has two children, but the ``NULL`` value of the second one is omitted, so its array has a single element. Parent 3 has no matching child rows at all, so its array is ``NULL`` rather than empty.

The ``child_by_pid_val`` index is what allows the in-call ``ORDER BY`` clause to be evaluated here. Since ``c.pid`` is equality-bound by the correlation, a scan of that index over one parent enumerates its children by ``val``, in reverse for ``DESC``.

.. _array-agg-important-notes:

Important notes
===============

* **Required indexes**: In general, ``GROUP BY`` queries require an appropriate index to be executed, and an in-call ``ORDER BY`` clause additionally requires that index to provide the requested order (see :ref:`ARRAY_AGG() with ORDER BY <array-agg-ordering>`). See :ref:`Indexes <index_definition>` for details on creating indexes that support ``GROUP BY`` operations.
* **ARRAY_AGG() in indexes**: ``ARRAY_AGG()`` itself cannot currently be materialized in an index. Defining an index over it, as in ``CREATE INDEX idx AS SELECT ARRAY_AGG(val) FROM tab GROUP BY grp``, raises an ``UNSUPPORTED_OPERATION`` error.
* **Group size**: ``ARRAY_AGG()`` assembles the array in memory as the rows of a group are read, and a query paused part-way through a group returns the elements collected so far in its continuation. Neither the array size in memory nor the continuation size is limited, and since they both grow with the group, a very large group can use a substantial amount of memory and produce a large continuation. Note that an ungrouped ``ARRAY_AGG()`` collects the entire input as a single group. You can use an in-call ``LIMIT`` clause to impose a hard bound on these sizes.
* **NULL handling**: An array cannot currently hold ``NULL`` elements. This is due to a limitation at the level of the FDB Record Layer, tracked by `Issue #3646 <https://github.com/FoundationDB/fdb-record-layer/issues/3646>`_. A query that uses the default ``RESPECT NULLS`` behavior (including when no null-treatment clause is present) will fail at run time with an ``UNSUPPORTED_OPERATION`` error as soon as a ``NULL`` is encountered. To avoid this potential error, use ``IGNORE NULLS`` to omit ``NULL`` values from the array.
* **Arrays of arrays**: An ``ARRAY``-typed argument would produce an array of arrays, which is not supported. ``ARRAY_AGG()`` over an ``ARRAY`` column raises an ``UNSUPPORTED_OPERATION`` error. This limitation is tracked by `Issue #4167 <https://github.com/FoundationDB/fdb-record-layer/issues/4167>`_. To collect nested collections, you can wrap the inner array in a struct, as in ``ARRAY_AGG((rid, tags))``.
* **DISTINCT**: The ``DISTINCT`` set quantifier is not supported yet. The parser accepts ``ARRAY_AGG(DISTINCT «expression» …)`` but raises an ``UNSUPPORTED_QUERY`` error. This limitation is tracked by `Issue #4499 <https://github.com/FoundationDB/fdb-record-layer/issues/4499>`_.
* **Subqueries**: ``ARRAY_AGG()`` may be used in a correlated ``FROM``-clause subquery, as shown in `ARRAY_AGG() in a correlated subquery`_ above, but not in a scalar subquery in the ``SELECT`` projection list. The latter, for example ``SELECT p.pid, (SELECT ARRAY_AGG(c.val IGNORE NULLS) FROM child c WHERE c.pid = p.pid) FROM parent p``, raises a ``SYNTAX_ERROR``. That is a general limitation of scalar subqueries in projections, not specific to ``ARRAY_AGG()``.
