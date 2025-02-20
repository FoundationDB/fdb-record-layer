=============
DROP DATABASE
=============

Drop a database. This drops all the schemas that are part of this database.

Syntax
======

.. raw:: html
    :file: DATABASE.diagram.svg

Parameters
==========

``pathToDatabase``
    The fully-qualified path of the ``DATABASE``

Example
=======

.. code-block:: sql

    -- Create database '/FRL/DEMO_DB'
    DROP DATABASE '/FRL/DEMO_DB'
