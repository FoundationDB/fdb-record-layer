=================
Logical Operators
=================

.. _logical-operators:

.. toctree::

    Logical/EXISTS

TODO: flesh out doc

.. list-table::
    :header-rows: 1

    * - Left Operand
      - Right Operand
      - OR
      - AND
      - XOR
    * - TRUE
      - NULL
      - TRUE
      - NULL
      - NULL
    * - FALSE
      - NULL
      - NULL
      - FALSE
      - NULL
    * - NULL
      - TRUE
      - TRUE
      - NULL
      - NULL
    * - NULL
      - FALSE
      - NULL
      - FALSE
      - NULL
    * - NULL
      - NULL
      - NULL
      - NULL
      - NULL
