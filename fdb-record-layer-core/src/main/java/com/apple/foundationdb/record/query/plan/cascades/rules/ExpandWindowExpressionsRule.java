/*
 * ExpandWindowExpressionsRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.WithValue;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * Rewrites a {@link SelectExpression} whose result or predicates contain a {@link CompileTimeWindowValue} into the
 * three levels a windowed index's match candidate is built as: a {@link SelectExpression} underneath a
 * {@link WindowExpression}, and a {@link SelectExpression} on top that projects the window result alongside the columns
 * the query still needs.
 *
 * <p>In other words, this rule is what eliminates the placeholder the SQL layer produced for a windowed call site. Before:
 * <pre>
 * SELECT (rank(q.a) OVER (PARTITION BY q.b) AS r, q.a AS a) over q
 * </pre>
 *
 * <p>and after:
 * <pre>
 * SELECT (w.0 AS r, w.2 AS a) over w
 *   WINDOW rank(s.1) OVER (), PARTITION BY s.0, ORDER BY ... over s
 *     SELECT (q.b, q.a) over q
 * </pre>
 *
 * <p>The lower select exists so that the query graph has the same shape as the candidate: without it the window expression
 * would range over whatever the original select ranged over, and the two could not line up. It also gives a window over
 * a join somewhere to put the join: it keeps <em>all</em> of the original select's quantifiers, so the window expression
 * always has exactly one input quantifier no matter how many the query joined.
 *
 * <p>The rule only fires for a select carrying exactly one window value. Several window functions in one select would each
 * need their own window expression, and deciding how to stack them is a separate problem.
 */
@API(API.Status.EXPERIMENTAL)
public class ExpandWindowExpressionsRule extends AbstractCascadesRule<SelectExpression>
        implements ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression();

    public ExpandWindowExpressionsRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var select = call.get(root);

        // there is no matcher for "a select that contains a window value", so the check happens here; a select without
        // one is the common case and is left alone
        final var windowValues = collectCompileTimeWindowValues(select);
        if (windowValues.size() != 1) {
            return;
        }
        final var compileTimeWindowValue = Iterables.getOnlyElement(windowValues);
        final var constantAliases = constantAliases(select);

        //
        // Every value below is expressed over the select's own quantifiers to begin with. The lower select keeps those
        // quantifiers, aliases and all, which is why nothing here has to be rebased: each level pulls the values it
        // needs up into the next one instead.
        //
        final var partitioningValues = distinctValues(compileTimeWindowValue.getPartitioningValues());

        //
        // Everything the select still flows that is not the window function itself has to be flowed by the window
        // expression too, or the upper select could not reference it any more -- except the partitioning columns, which
        // the window expression flows on its own. Listing one here as well would flow the same column twice.
        //
        final var additionalResultValues = distinctValues(
                select.getResultValues()
                        .stream()
                        .filter(resultValue -> !containsCompileTimeWindowValue(resultValue))
                        .filter(resultValue -> partitioningValues.stream()
                                .noneMatch(partitioningValue ->
                                        partitioningValue.semanticEquals(resultValue, AliasMap.emptyMap())))
                        .collect(ImmutableList.toImmutableList()));

        //
        // The lower select flows the partitioning columns first, which lines their ordinals up with the way the match
        // candidate expands a windowed index key, then what the window function itself reads, then what the query still
        // selects. Each column flows exactly once: pulling a value up looks it up among the flowed columns, and two
        // semantically equal columns make that lookup ambiguous. A column can be duplicated both because it is
        // partitioned by and selected ("rank(a) OVER (PARTITION BY b), b") and because the query selects it twice.
        //
        final var lowerValues = distinctValues(
                Iterables.concat(partitioningValues,
                        compileTimeWindowValue.getArgumentValues(),
                        orderingValues(compileTimeWindowValue),
                        additionalResultValues));

        //
        // A predicate that does not mention the window function filters rows before the function is computed, so it
        // belongs underneath the window expression. PredicatePushDownRule already pushes a predicate over a single
        // quantifier further down than this, but it cannot move one that spans several, so a join predicate reaches its
        // place only here. A predicate on the window value itself cannot be evaluated before that value exists and so
        // stays on top.
        //
        final var lowerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var upperPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        for (final var predicate : select.getPredicates()) {
            if (containsCompileTimeWindowValue(predicate)) {
                upperPredicatesBuilder.add(predicate);
            } else {
                lowerPredicatesBuilder.add(predicate);
            }
        }

        //
        // The lower select is a final expression, so it has to range over final expressions as well: each of the
        // matched select's quantifiers is rebound to a reference holding only the final members of the one it ranged
        // over. Rebinding keeps the alias, so everything expressed over these quantifiers stays valid.
        //
        final var lowerQuantifiers = select.getQuantifiers()
                .stream()
                .map(quantifier -> {
                    final var childReference = quantifier.getRangesOver();
                    return quantifier.overNewReference(
                            call.memoizeFinalExpressionsFromOther(childReference,
                                    childReference.getFinalExpressions()));
                })
                .collect(ImmutableList.toImmutableList());

        final var lowerSelect = new SelectExpression(RecordConstructorValue.ofUnnamed(lowerValues),
                lowerQuantifiers, lowerPredicatesBuilder.build());
        final var lowerQuantifier = Quantifier.forEach(call.memoizeFinalExpression(lowerSelect));
        final var lowerResultValue = lowerSelect.getResultValue();

        //
        // The window expression ranges over the lower select, so everything handed to it has to be expressed over that
        // quantifier rather than over the quantifiers the values were originally built against -- including the
        // requested ordering, which would otherwise stay correlated to an alias that is not in scope for it.
        //
        final var windowValue = (WindowValue)compileTimeWindowValue.toWindowValue()
                .withChildren(pullUpAll(compileTimeWindowValue.getArgumentValues(), lowerResultValue, lowerQuantifier,
                        constantAliases));
        final var windowExpression = new WindowExpression(windowValue,
                pullUpAll(partitioningValues, lowerResultValue, lowerQuantifier, constantAliases),
                pullUpAll(additionalResultValues, lowerResultValue, lowerQuantifier, constantAliases),
                pullUpOrderingParts(compileTimeWindowValue.getOrderingParts(), lowerResultValue,
                        lowerQuantifier, constantAliases),
                lowerQuantifier);

        final var windowQuantifier = Quantifier.forEach(call.memoizeFinalExpression(windowExpression));
        final var windowResultValue = windowExpression.getResultValue();

        //
        // The upper select reads the window expression's columns positionally. Its values start out over the original
        // quantifiers, so they travel up both levels: through the lower select, then through the window expression.
        //
        final var upperResultValue = pullUpThroughWindow(select.getResultValue(), lowerResultValue, lowerQuantifier,
                windowResultValue, windowQuantifier, constantAliases);
        final var upperPredicates = upperPredicatesBuilder.build()
                .stream()
                .map(predicate -> pullUpPredicate(predicate, lowerResultValue, lowerQuantifier, windowResultValue,
                        windowQuantifier, constantAliases))
                .collect(ImmutableList.toImmutableList());

        call.yieldFinalExpression(
                new SelectExpression(upperResultValue, ImmutableList.of(windowQuantifier), upperPredicates));
    }

    /**
     * The aliases the values in the given select may legitimately stay correlated to: whatever its inputs are
     * correlated to from the outside. The select's own quantifiers are not among them -- those are exactly the aliases
     * the pull-ups translate away.
     */
    @Nonnull
    private static Set<CorrelationIdentifier> constantAliases(@Nonnull final SelectExpression select) {
        return select.getQuantifiers()
                .stream()
                .flatMap(quantifier -> quantifier.getRangesOver().getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    private static List<Value> orderingValues(@Nonnull final CompileTimeWindowValue compileTimeWindowValue) {
        return compileTimeWindowValue.getOrderingParts()
                .stream()
                .map(WindowOrderingPart::getValue)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Collects the distinct {@link CompileTimeWindowValue}s the given select computes, whether they appear in its result
     * value or inside one of its predicates.
     */
    @Nonnull
    private static List<CompileTimeWindowValue> collectCompileTimeWindowValues(@Nonnull final SelectExpression select) {
        return Stream.concat(
                        select.getResultValue().preOrderStream(),
                        select.getPredicates()
                                .stream()
                                .flatMap(ExpandWindowExpressionsRule::predicateValues)
                                .flatMap(Value::preOrderStream))
                .filter(CompileTimeWindowValue.class::isInstance)
                .map(CompileTimeWindowValue.class::cast)
                .distinct()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * The values the given predicate computes, looking inside compound predicates as well: a window value can sit in
     * either conjunct of an {@code AND}.
     */
    @Nonnull
    private static Stream<Value> predicateValues(@Nonnull final QueryPredicate predicate) {
        return predicate.preOrderStream()
                .filter(WithValue.class::isInstance)
                .map(nestedPredicate -> (WithValue<?>)nestedPredicate)
                .map(WithValue::getValue)
                .filter(Objects::nonNull);
    }

    /**
     * Returns whether the given value computes a window function anywhere inside it. The test has to look at the whole
     * tree rather than just the value itself: {@code rank(a) + 1} is not a window value, but it is not a column that can
     * be flowed as-is either.
     */
    private static boolean containsCompileTimeWindowValue(@Nonnull final Value value) {
        return value.preOrderStream().anyMatch(CompileTimeWindowValue.class::isInstance);
    }

    private static boolean containsCompileTimeWindowValue(@Nonnull final QueryPredicate predicate) {
        return predicateValues(predicate).anyMatch(ExpandWindowExpressionsRule::containsCompileTimeWindowValue);
    }

    /**
     * The given values with semantic duplicates removed, keeping the first occurrence of each.
     */
    @Nonnull
    private static List<Value> distinctValues(@Nonnull final Iterable<? extends Value> values) {
        final var distinctValues = new ArrayList<Value>();
        for (final var value : values) {
            if (!containsSemanticEqual(distinctValues, value)) {
                distinctValues.add(value);
            }
        }
        return ImmutableList.copyOf(distinctValues);
    }

    private static boolean containsSemanticEqual(@Nonnull final Iterable<? extends Value> values,
                                                 @Nonnull final Value value) {
        return Streams.stream(values)
                .anyMatch(existingValue -> existingValue.semanticEquals(value, AliasMap.emptyMap()));
    }

    @Nonnull
    private static QueryPredicate pullUpPredicate(@Nonnull final QueryPredicate predicate,
                                                  @Nonnull final Value lowerResultValue,
                                                  @Nonnull final Quantifier lowerQuantifier,
                                                  @Nonnull final Value windowResultValue,
                                                  @Nonnull final Quantifier windowQuantifier,
                                                  @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        if (!(predicate instanceof PredicateWithValue)) {
            return predicate;
        }
        final var predicateWithValue = (PredicateWithValue)predicate;
        final var value = predicateWithValue.getValue();
        if (value == null) {
            return predicate;
        }
        return predicateWithValue.withValue(pullUpThroughWindow(value, lowerResultValue, lowerQuantifier,
                windowResultValue, windowQuantifier, constantAliases));
    }

    /**
     * Expresses a value written over the original quantifiers over the window expression's output instead, and replaces
     * the placeholder window value with a reference to the column the window expression flows it in.
     */
    @Nonnull
    private static Value pullUpThroughWindow(@Nonnull final Value value,
                                             @Nonnull final Value lowerResultValue,
                                             @Nonnull final Quantifier lowerQuantifier,
                                             @Nonnull final Value windowResultValue,
                                             @Nonnull final Quantifier windowQuantifier,
                                             @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var overLowerSelect = pullUp(value, lowerResultValue, lowerQuantifier, constantAliases);
        final var overWindow = pullUp(overLowerSelect, windowResultValue, windowQuantifier, constantAliases);
        return Objects.requireNonNull(
                overWindow.replace(part -> part instanceof CompileTimeWindowValue
                                           ? windowValueReference(windowResultValue, windowQuantifier)
                                           : part));
    }

    @Nonnull
    private static List<Value> pullUpAll(@Nonnull final Iterable<? extends Value> values,
                                         @Nonnull final Value lowerResultValue,
                                         @Nonnull final Quantifier lowerQuantifier,
                                         @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return Streams.stream(values)
                .map(value -> pullUp(value, lowerResultValue, lowerQuantifier, constantAliases))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static List<WindowOrderingPart> pullUpOrderingParts(
            @Nonnull final Iterable<WindowOrderingPart> orderingParts,
            @Nonnull final Value lowerResultValue,
            @Nonnull final Quantifier lowerQuantifier,
            @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return Streams.stream(orderingParts)
                .map(orderingPart -> new WindowOrderingPart(
                        pullUp(orderingPart.getValue(), lowerResultValue, lowerQuantifier, constantAliases),
                        orderingPart.getSortOrder()))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Translates a value to reference the columns the given quantifier flows.
     */
    @Nonnull
    private static Value pullUp(@Nonnull final Value value,
                                @Nonnull final Value lowerResultValue,
                                @Nonnull final Quantifier lowerQuantifier,
                                @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return MaxMatchMap.compute(value, lowerResultValue, constantAliases)
                .translateQueryValueMaybe(lowerQuantifier.getAlias())
                .orElseThrow(() -> new RecordCoreException("value cannot be pulled up"));
    }

    /**
     * Returns a reference to the column the window expression flows its {@link WindowValue} in.
     */
    @Nonnull
    private static FieldValue windowValueReference(@Nonnull final Value windowResultValue,
                                                  @Nonnull final Quantifier windowQuantifier) {
        final var columns = Values.deconstructRecord(windowResultValue);
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) instanceof WindowValue) {
                return FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(windowQuantifier), i);
            }
        }
        throw new RecordCoreException("window expression does not flow a window value");
    }
}
