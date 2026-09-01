# Comments

SQL comments let you annotate a statement. They are discarded during lexical analysis and have no effect on results, query planning, or query caching: a statement behaves exactly as if its comments were not there.

The Relational Layer supports two comment styles:

- **Line comments** start with `--` and continue to the end of the line.
- **Block comments** are delimited by `/*` and `*/`, may span multiple lines, and may be nested.

```sql
-- Select the premium products.
SELECT name FROM products WHERE price > 100 ORDER BY name;

SELECT name /* only the name */ FROM products
WHERE price > 100 /* this predicate
                     spans multiple lines */
ORDER BY name;
```

Following the ANSI SQL and PostgreSQL convention, `--` begins a comment even when it is not followed by whitespace, so `SELECT 1--comment` is equivalent to `SELECT 1`.

A line comment ends at the first line break, which may be a line feed, a carriage return, or a carriage return followed by a line feed, or at the end of the statement if no line break follows.

Block comments nest: a `/*` inside a block comment starts an inner comment that must be closed before the enclosing one. A region containing comments can therefore be commented out as a whole.

```sql
SELECT name /* outer /* inner */ still a comment */ FROM products;
```

A block comment that is never closed is a syntax error, rather than a comment that runs to the end of the statement.

Comments are only recognized outside of string literals and quoted identifiers. The sequences `--` and `/*` inside a quoted string are ordinary data, so the literal `'a -- b'` retains its value verbatim, and a quoted identifier such as `"a--b"` keeps its exact spelling.

```{note}
The Relational Layer does **not** support `#` line comments or `/*! ... */` executable comments, which some SQL dialects provide. Outside of a string literal, `#` is a syntax error, and a `/*! ... */` sequence is treated as an ordinary block comment (its contents are ignored rather than executed).
```

Because comments never reach the query engine, they cannot be used to pass hints to the planner.
