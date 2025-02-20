===========
IS Operator
===========

.. _is-operators:

TODO: flesh out doc

For predicates on nullable fields, we support :sql:`IS [NOT] NULL`, :sql:`IS [NOT] UNKNOWN` operator, and
also :sql:`IS [NOT] true/false`. :sql:`NULL` and :sql:`UNKNOWN` are synonyms in these clauses. For Boolean
value fields, :sql:`IS [NOT] true/false` can be used to assume default values for unset fields during Boolean
predicates. For instance,
:sql:`x IS NOT true` is equivalent to :sql:`x IS NULL OR x = FALSE` (effectively treating unset fields as equivalent
to :sql:`false`).

