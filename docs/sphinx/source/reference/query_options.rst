=============
Query options
=============

Query options are a comma-separated list of directives that can be attached to a query to influence
how it is planned or executed. They are supplied through an ``OPTIONS`` clause placed at the end of
the query:

.. code-block:: sql

    SELECT ... OPTIONS (<option> [, <option> ...])

Options affect only the query they are attached to; they do not change any connection- or
transaction-wide settings. The pages in this section describe the individual options that are
available.

.. toctree::
    :maxdepth: 1

    query_options/ISOLATION_LEVEL_SNAPSHOT
