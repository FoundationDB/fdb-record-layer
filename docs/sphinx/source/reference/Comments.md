# Comments

SQL comments let you annotate a statement. They are discarded during lexical analysis and have no effect on results, query planning, or query caching: a statement behaves exactly as if its comments were not there.

The Relational Layer supports two comment styles:

- **Line comments** start with `--` and continue to the end of the line.
- **Block comments** are delimited by `/*` and `*/` and may span multiple lines.

```sql
-- Select the premium products.
SELECT name FROM products WHERE price > 100 ORDER BY name;

SELECT name /* only the name */ FROM products
WHERE price > 100 /* this predicate
                     spans multiple lines */
ORDER BY name;
```

Following the ANSI SQL and PostgreSQL convention, `--` begins a comment even when it is not followed by whitespace, so `SELECT 1--comment` is equivalent to `SELECT 1`.

Comments are only recognized outside of string literals. The sequences `--` and `/*` inside a quoted string are ordinary data, so the literal `'a -- b'` retains its value verbatim.

```{note}
Unlike MySQL, the Relational Layer does **not** support `#` line comments or `/*! ... */` executable comments. Outside of a string literal, `#` is a syntax error, and a `/*! ... */` sequence is treated as an ordinary block comment (its contents are ignored rather than executed).
```

Because comments never reach the query engine, they cannot be used to pass hints to the planner.
