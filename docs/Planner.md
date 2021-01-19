## AST matching mechanics

### Example 1

Here is a query graph (green theme) and an index scan graph (red theme). We would like to match the query graph against that index graph. In this case the index scan graph uses an outside parameter that needs to be filled in as a byproduct of the matching. The second byproduct is the necessary compensation of the match. The query graph is independent of any outside parameters.
For a human it’s pretty straightforward to squint a little and see how the red graph fits into the green graph. The challenge here is to achieve the same algorithmically and efficiently.

<img src="QGM%20-%20matching%201.png" width="1000">

### General strategy

We match an index scan graph against a query graph bottom-up. The reason for that is that we want to make use
of the memoization structures already used during planning. We also try to naturally employ the ideas around the
Cascades planner to do matching, i.e., use the mechanics of the engine that already walks and transforms the query graph
during planning to our advantage for index matching as well.
Thus we match the leaves of the graphs first, then proceed to expressions higher up. At each matching step we make the
information available to the memoization structures in order to aid other subsequent matching attempts. We call that
auxiliary structure a partial match. We associate a partial match if an `ExpressionRef`, as the match represents the
entire graph that is contained in that reference even though the match actually matched only a subset or even just one
of its contained `RelationExpression`s.
Another important point to note is how we deal with correlations. A correlation is expressed through a
`CorrelationIdentifier` (or alias) used by a producing quantifier. That same `CorrelationIdentifier` is then used by all
expressions that functionally depend on a bound item from that quantifier. The identifier’s value is irrelevant as long
as it is unique and the consumer and producer use the same identifier. A correlation can also be viewed as a parameter
to a function that represents the execution of the sub-graph that is correlated to some quantifier outside of
that sub-graph. As an example, the sub-graph of the green graph rooted at the SELECT expression that filters for `x > 5`
is correlated to a quantifier `q2`. We can also just say that sub-graph is a function `f: INT → REL`
(producing a relation for an integer handed in). On the index scan graph side (the entire reddish graph), the graph is
dependent on a supplied value (to indicate the search argument of the eventual index scan). A problem during matching
(and specifically during bottom-up matching) is to understand which correlation on the query graph side corresponds to
which correlation on the index scan graph side. If we look at one of the table functions in the pictures (and let’s assume
these table function iterate through and produce the nested repeated fields of some record), those table functions are
correlated to a quantifier. Therefore, at execution time each invocation of the table function is bound to a record.
The correlation is anchored somewhere above in that graph. In the context of just matching e.g. two table functions
(left TFs in each graph) without further surrounding information, e.g. the table function (TF) `nested_fields(q2)` on the
querying side versus `nested_fields(qa)` on the querying side, it is inherently impossible to reason if these two table
functions match without knowing if `q2` is the same as `qa` or not. We can only know that once we reach a higher level
during matching. In the example, it is correct to conclude `q2` is equivalent to `qa` once we match the select expressions
containing `q1` respectively `qa`. Therefore, for the left of the two table functions in the respective graphs the table
functions do match in that case. However, that would not be the case for the right one on the query graph side (green) side
compared with the left one on the index scan graph side (red). We again wouldn’t know (given that they let’s say both do
a nested_fields() operation) that `nested_fields(q4)` is not equivalent to `nested_fields(qa)` when we attempt to match
the table functions as we don’t understand yet if there is a correspondence between `q4` and `qa` (which there is not).
As a consequence of this observation we need to incorporate the assumptions we have about the equivalence of correlation
sources into our partial match structure and see if they hold up later. `nested_fields(q2)` can be matched to
`nested_fields(qa)` under the assumption that `q2` is equivalent to `qa`. Later it can be proven that `q2` is in fact
equivalent to `qa` and the assumption holds. The opposite can be done for our example that eventually would not produce
a match. `nested_fields(q4)` can be matched to `nested_fields(qa)` under the assumption that `q4` is equivalent to `qa`.
Later on we will realize and disprove that but when we only try to match bottom up that decision will have to be deferred.
In the remainder we will use the phrase: ... partial match under mapping `q4 → qa`...
On the way up from the leaves to the root of the respective query DAGs we can match more and more of the graphs if
that’s at all possible.
For a non-leaf expression we will only be able to match it to the corresponding expression on the index scan graph side
if all children are partially matched under compatible correlation mappings. If there is no such mapping where a common
partial match may exist or the current expression at hand is structurally not equivalent (or more general subsuming),
the current expression cannot be matched and matching for this particular index scan graph is impossible for parents
of this expression. If, on the other hand, we reach the top-most expression on the index scan graph, we matched the index
scan graph entirely, and can replace the index scan graph with an index scan operator.
While we match query graph to index scan graph we may encounter a predicate (or a some other sort of scalar expression)
that is specific on the query side, i.e. `x > 5` in the example and it needs to match a comparison range on the index
scan graph side. In this case the field expression `x` together with a comparison range is considered special as it
acts like a wildcard or placeholder. It matches any actual predicate on the query side using that field `x`. In this
case it would match `x` with the comparison range `[5, Inf]`. That comparison range is entirely synthesized from the query side.
At that moment we can conclude that if we get a match at all, we can equate a parameter `:p1` to that
comparison range . That is crucial when the index scan graph is replaced by an index scan operator as that operator
needs to know the search argument.

### Matching the leaves

<img src="QGM%20-%20matching%202.png" width="600">

We attempt to match the leaves of both graphs first. Everything that is related to a matching structure or that is
matched is depicted in a blue shade. In our example the scan over A is not dependent on any correlation. It matches
because both sides select from `A`. The table functions `TF(a)` and `TF(x)` match under the mappings `q2 → qa` and
`q4 → qc` respectively. Note that `TF(x)` would also match `TF(a)` under the mapping `q4 → qa`, which would later be
disproven to be matchable so it is not shown in the picture. There could be additional partial matches as well that
will eventually also not lead to a proper match. These other incorrect partial matches are also not shown.
This matching step is invoked for `RelationExpression`s as part of a query transformation rule called `MatchLeafRule`.

### Matching intermediate expressions

In the example, we attempt to match the `SELECT` expression (with `x > 5`) on the query side with the `SELECT` expression
(with x COMPARISONRANGE) on the index scan graph side. First, in order to match this expression successfully we need to
find a mapping to all unbound correlated aliases that this SELECT is correlated to that is in turn compatible to a
partial match from each child of the SELECT. An `AliasMap` m is compatible to an AliasMap `m’` if all mappings `m` do not
conflict. A conflict would be a mapping `q → q’` and a mapping `q → q"` where `q' != q`". We then can compute matches
for all remaining conflict-free mappings. We need to establish structural equivalence between the SELECT expressions
(are they in fact both expressions of the same kind?; do they have the same kind of predicates?) and if needed record
parameter mappings. In our case, the `SELECT` expressions match under the mapping `q2 → qa` (we were able to prove
that `q4` and `qc` where indeed equivalent which we can now drop) and with the bound parameter `:p1` bound to `> 5`.
This matching step can be invoked for `RelationExpression`s as part of a query transformation rule runs after the leaves
are matched which by design is suited to the way the Cascades planner explores the query graph.

<img src="QGM%20-%20matching%203.png" width="600">

### Matching the top level expression

In principle we match the top level expression in the same way as any other expression. We should just use the
appropriate matching rulea and if appropriate then invoke a rule that is triggered upon reaching the top of the
to-be-matched index scan graph.

<img src="QGM%20-%20matching%204.png" width="450">

The match is now complete. One of its byproducts is the fact that `:p1` is bound to `> 5`. The partial match information
also keeps information about how to relate the index scan graph back to an actual operator that preforms the actual scan.
Using the information about bound parameters the index scan operator can now replace the matched blue part in the query QGM.

<img src="QGM%20-%20matching%205.png" width="150">
