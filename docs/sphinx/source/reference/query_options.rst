=============
Query options
=============

Query options are a comma-separated list of directives that influence how a statement is planned or
executed. They are supplied through an ``OPTIONS`` clause:

.. code-block:: sql

    <statement> OPTIONS (<option> [, <option> ...])

Scope
#####

An ``OPTIONS`` clause is **statement-level**: it applies to the statement as a whole, not to an
individual clause within it. It is written once, at the end of a top-level statement — a ``SELECT``,
``INSERT``, ``UPDATE``, ``DELETE``, or ``EXECUTE CONTINUATION``. It cannot be attached to a nested
query such as a subquery, a common table expression, or an index definition; placing ``OPTIONS``
anywhere other than at the end of a top-level statement is a syntax error.

An option affects only the statement it is attached to. It does not change any connection- or
transaction-wide setting, and it is not remembered for later statements — each statement that needs
an option must specify it.

Which options are valid depends on the statement: some apply only to read-only ``SELECT`` queries,
others only to data-modifying statements. See each option's page for details.

.. toctree::
    :maxdepth: 1

    query_options/ISOLATION_LEVEL_SNAPSHOT
