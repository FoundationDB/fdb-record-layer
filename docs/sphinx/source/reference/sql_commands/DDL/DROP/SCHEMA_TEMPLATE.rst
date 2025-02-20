====================
DROP SCHEMA TEMPLATE
====================

Drops a schema template.

``NOTE``: This is still under active development and is experimental. The behavior of ``DROP`` is not clearly defined or tested in case there are still schemas that are defined with the schema template that we want to delete.

Syntax
======

.. raw:: html
    :file: SCHEMA_TEMPLATE.diagram.svg

Parameters
==========

``schemaTemplateName``
    The name of the schema template to be droped.

Example
=======

.. code-block:: sql

    -- drop schema template DEMO_SCHEMA_TEMPLATE
    DROP SCHEME TEMPLATE DEMO_SCHEMA_TEMPLATE
