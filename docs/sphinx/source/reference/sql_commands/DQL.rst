===
DQL
===

The Relational Layer supports a full query engine, complete with query planning and index selection. We generally recommend
using the query planner whenever possible over using the :doc:`../Direct_Access_Api` as it is more powerful and more capable, and
it is designed to make intelligent choices regarding query planning decisions like appropriate index selection.

The language is heavily inspired by SQL. However, the Relational Layer *does* make some important changes to SQL to accommodate
the nature of its data model (particularly with regard to :ref:`struct <struct_types>` and :ref:`array <array_types>` data types).

Syntax
######

.. raw:: html
    :file: DQL.diagram.svg

.. toctree::
    :maxdepth: 2

    DQL/SELECT
    DQL/WITH
    DQL/WHERE
    DQL/GROUP_BY
    DQL/ORDER_BY
    DQL/EXPLAIN

.. toctree::
    :maxdepth: 2

    DQL/Operators
