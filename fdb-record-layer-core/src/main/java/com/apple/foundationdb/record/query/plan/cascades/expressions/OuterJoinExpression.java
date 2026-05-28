/*
 * OuterJoinExpression.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.WithIndentationsExplainFormatter;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A logical expression representing an outer join between exactly two inputs.
 *
 * <p>This expression can represent either a {@code LEFT OUTER JOIN} or a {@code RIGHT OUTER JOIN}. The two children are
 * named after their role (rather than the SQL join direction):
 * <ul>
 *     <li>The <em>preserved</em> quantifier supplies the rows that always make it into the output.</li>
 *     <li>The <em>null-supplying</em> quantifier supplies rows that are matched against the preserved side; whenever
 *     no match exists, a null record is flowed.</li>
 * </ul>
 *
 * <p>Join-condition predicates (from the SQL {@code ON} clause) are stored explicitly in this expression and can be
 * accessed through {@link #getJoinPredicates()}. Filter predicates (from the {@code WHERE} clause) are not stored here;
 * they belong in an enclosing {@link SelectExpression}. Since that {@code ON} predicates have different semantics from
 * {@code WHERE} predicates, {@code OuterJoinExpression} does not implement {@link RelationalExpressionWithPredicates}.
 */
@API(API.Status.EXPERIMENTAL)
public class OuterJoinExpression extends AbstractRelationalExpressionWithChildren
        implements InternalPlannerGraphRewritable {

    /** The preserved-side quantifier of the join. */
    @Nonnull
    private final Quantifier.ForEach preservedQuantifier;

    /** The null-supplying-side quantifier of the join. */
    @Nonnull
    private final Quantifier.ForEach nullSupplyingQuantifier;

    /** Predicates from the SQL {@code ON} clause that determine which rows match across the two sides. */
    @Nonnull
    private final List<? extends QueryPredicate> joinPredicates;

    /** The combined result value flowing columns from both sides. */
    @Nonnull
    private final Value resultValue;

    /** A memoized mapping from correlation alias to the quantifier that owns it. */
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Map<CorrelationIdentifier, ? extends Quantifier>> aliasToQuantifierMapSupplier;

    /** A memoized partial order capturing correlation dependencies between the two quantifiers. */
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<PartiallyOrderedSet<CorrelationIdentifier>> correlationOrderSupplier =
            Suppliers.memoize(this::computeCorrelationOrder);

    /**
     * Creates a new outer join expression.
     *
     * <p><b>Precondition:</b> Neither {@code preservedQuantifier} nor {@code nullSupplyingQuantifier} may have
     * {@link Quantifier.ForEach#isNullOnEmpty() null-on-empty} set.
     *
     * @param preservedQuantifier the preserved-side for-each quantifier
     * @param nullSupplyingQuantifier the null-supplying-side for-each quantifier
     * @param joinPredicates predicates from the {@code ON} clause
     * @param resultValue value combining columns from both sides
     */
    @SuppressWarnings("this-escape")
    public OuterJoinExpression(@Nonnull final Quantifier.ForEach preservedQuantifier,
                               @Nonnull final Quantifier.ForEach nullSupplyingQuantifier,
                               @Nonnull final List<? extends QueryPredicate> joinPredicates,
                               @Nonnull final Value resultValue) {
        Verify.verify(!preservedQuantifier.isNullOnEmpty());
        Verify.verify(!nullSupplyingQuantifier.isNullOnEmpty());
        this.preservedQuantifier = preservedQuantifier;
        this.nullSupplyingQuantifier = nullSupplyingQuantifier;
        this.joinPredicates = ImmutableList.copyOf(joinPredicates);
        this.resultValue = resultValue;
        this.aliasToQuantifierMapSupplier = Suppliers.memoize(
                () -> Quantifiers.aliasToQuantifierMap(getQuantifiers()));
    }

    /** Returns the preserved-side quantifier. */
    @Nonnull
    public Quantifier.ForEach getPreservedQuantifier() {
        return preservedQuantifier;
    }

    /** Returns the null-supplying-side quantifier. */
    @Nonnull
    public Quantifier.ForEach getNullSupplyingQuantifier() {
        return nullSupplyingQuantifier;
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    /**
     * Returns the {@code ON}-clause join predicates.
     */
    @Nonnull
    public List<? extends QueryPredicate> getJoinPredicates() {
        return joinPredicates;
    }

    /**
     * Returns the two quantifiers. The first quantifier is for the preserved side, the second is for the
     * null-supplying side.
     */
    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(preservedQuantifier, nullSupplyingQuantifier);
    }

    /**
     * {@inheritDoc}
     *
     * @return Always returns {@code 2}, as an outer join is strictly binary.
     */
    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    /**
     * {@inheritDoc}
     *
     * @return Always returns {@code true} because the null-supplying side is typically correlated to the preserved side
     * via the join predicates.
     */
    @Override
    public boolean canCorrelate() {
        return true;
    }

    /** Returns a memoized mapping from the alias of each quantifier to the quantifier itself. */
    @Nonnull
    public Map<CorrelationIdentifier, ? extends Quantifier> getAliasToQuantifierMap() {
        return aliasToQuantifierMapSupplier.get();
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public PartiallyOrderedSet<CorrelationIdentifier> getCorrelationOrder() {
        return correlationOrderSupplier.get();
    }

    /**
     * Computes the partial order of correlation dependencies between the two quantifiers by examining the
     * {@code getCorrelatedTo()} set of each quantifier.
     */
    @Nonnull
    private PartiallyOrderedSet<CorrelationIdentifier> computeCorrelationOrder() {
        final Map<CorrelationIdentifier, ? extends Quantifier> aliasToQuantifierMap = getAliasToQuantifierMap();
        return PartiallyOrderedSet.of(
                getQuantifiers().stream()
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet()),
                alias -> Objects.requireNonNull(aliasToQuantifierMap.get(alias)).getCorrelatedTo());
    }

    /**
     * Returns correlations referenced by the join predicates or result value that are not satisfied by the owned
     * quantifiers (i.e., outer correlations).
     */
    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return Streams.concat(
                        joinPredicates.stream().flatMap(p -> p.getCorrelatedTo().stream()),
                        resultValue.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Creates a new {@code OuterJoinExpression} with all correlations translated according to the given map.
     */
    @Nonnull
    @Override
    public OuterJoinExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues,
                                                     @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        final Quantifier.ForEach translatedPreserved = translatedQuantifiers.get(0).narrow(Quantifier.ForEach.class);
        final Quantifier.ForEach translatedNullSupplying = translatedQuantifiers.get(1).narrow(Quantifier.ForEach.class);
        // Don’t allocate a new expression if the translation is a no-op and the children translated to equal quantifiers.
        if (translationMap.definesOnlyIdentities()
                && translatedPreserved.equals(preservedQuantifier)
                && translatedNullSupplying.equals(nullSupplyingQuantifier)) {
            return this;
        }
        final List<QueryPredicate> translatedPredicates =
                joinPredicates.stream()
                        .map(p -> p.translateCorrelations(translationMap, shouldSimplifyValues))
                        .collect(Collectors.toList());
        final Value translatedResultValue =
                resultValue.translateCorrelations(translationMap, shouldSimplifyValues);
        return new OuterJoinExpression(
                translatedPreserved,
                translatedNullSupplying,
                translatedPredicates,
                translatedResultValue);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Two {@code OuterJoinExpression}s are equal without children if their result values and join predicates are
     * semantically equal under the given alias mapping.
     */
    @Override
    @SuppressWarnings({"UnstableApiUsage", "PMD.CompareObjectsWithEquals"})
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final OuterJoinExpression other = (OuterJoinExpression) otherExpression;
        return semanticEqualsForResults(otherExpression, aliasMap) &&
                joinPredicates.size() == other.joinPredicates.size() &&
                Streams.zip(joinPredicates.stream(),
                                other.joinPredicates.stream(),
                                (p, op) -> p.semanticEquals(op, aliasMap))
                        .allMatch(isSame -> isSame);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(joinPredicates, resultValue);
    }

    /**
     * Produces a planner-graph node labeled {@code OUTER JOIN} with the {@code ON} predicate as detail text.
     */
    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final List<String> details;
        if (joinPredicates.isEmpty()) {
            details = ImmutableList.of();
        } else {
            final WithIndentationsExplainFormatter formatter = WithIndentationsExplainFormatter.forDot(7);
            final String predicateString =
                    "ON " + AndPredicate.and(joinPredicates).explain()
                            .getExplainTokens()
                            .render(formatter);
            details = ImmutableList.of(predicateString);
        }

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this, "OUTER JOIN", details, ImmutableMap.of()),
                childGraphs);
    }
}
