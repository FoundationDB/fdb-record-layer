============
CREATE INDEX
============

Clause in a :ref:`schema template definition <create-schema-template>` to create an index. The syntax here takes
a query and is inspired by materialized view syntax. However, unlike a general `VIEW`, the index must be able to
maintained incrementally. That means that for any given record insert, update, or delete, it must be possible
to construct the difference that needs to be applied to each index in order to update it without needing to
completely rebuild it. That means that there are a number of limitations to the `query` argument. See
:ref:`index_definition` for more details.

Syntax
======

.. raw:: html
    :file: INDEX.diagram.svg

Parameters
==========

``indexName``
    The name of the index. Note that the name of the index should be unique in the schema template.
