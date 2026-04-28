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
 * <p>The {@link JoinType JoinType} determines which sides are “null-supplying”:
 * <ul>
 *     <li>{@link JoinType#LEFT LEFT} — The left quantifier is <em>preserved</em>; the right is <em>null-supplying</em>.</li>
 *     <li>{@link JoinType#RIGHT RIGHT} — The right quantifier is <em>preserved</em>; the left is <em>null-supplying</em>.</li>
 *     <li>{@link JoinType#FULL FULL} — Both sides are null-supplying with respect to the other.</li>
 * </ul>
 *
 * <p>Join-condition predicates (from the SQL {@code ON} clause) are stored explicitly in this expression and can be
 * accessed through {@link #getJoinPredicates()}. Filter predicates (from the {@code WHERE} clause) are not stored here;
 * they belong in an enclosing {@link SelectExpression}. Note that ON predicates have different semantics from WHERE
 * predicates: they determine match/null-padding rather than row filtering. For this reason {@code OuterJoinExpression}
 * does not implement {@link RelationalExpressionWithPredicates}.
 *
 * <p><b>Current limitation:</b> Only {@link JoinType#LEFT LEFT} joins are supported end-to-end. {@code RIGHT} and
 * {@code FULL} are defined here to allow the model to represent them, but no planning rules support them yet.
 */
@API(API.Status.EXPERIMENTAL)
public class OuterJoinExpression extends AbstractRelationalExpressionWithChildren
        implements InternalPlannerGraphRewritable {

    /**
     * A type of outer join.
     */
    public enum JoinType {
        /** Left outer join: left side is preserved, right side is null-supplying. */
        LEFT("LEFT OUTER JOIN"),
        /** Right outer join: right side is preserved, left side is null-supplying. */
        RIGHT("RIGHT OUTER JOIN"),
        /** Full outer join: both sides are null-supplying with respect to the other. */
        FULL("FULL OUTER JOIN");

        @Nonnull
        private final String displayName;

        JoinType(@Nonnull final String displayName) {
            this.displayName = displayName;
        }

        /** Returns a human-readable label for use in {@code EXPLAIN} output. */
        @Nonnull
        public String getDisplayName() {
            return displayName;
        }
    }

    /** The type of outer join. */
    @Nonnull
    private final JoinType joinType;

    /** The left quantifier of the join. */
    @Nonnull
    private final Quantifier.ForEach leftQuantifier;

    /** The right quantifier of the join. */
    @Nonnull
    private final Quantifier.ForEach rightQuantifier;

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
     * @param joinType the kind of outer join
     * @param leftQuantifier the left-side quantifier
     * @param rightQuantifier the right-side quantifier
     * @param joinPredicates predicates from the {@code ON} clause
     * @param resultValue value combining columns from both sides
     */
    @SuppressWarnings("this-escape")
    public OuterJoinExpression(@Nonnull final JoinType joinType,
                               @Nonnull final Quantifier.ForEach leftQuantifier,
                               @Nonnull final Quantifier.ForEach rightQuantifier,
                               @Nonnull final List<? extends QueryPredicate> joinPredicates,
                               @Nonnull final Value resultValue) {
        this.joinType = joinType;
        this.leftQuantifier = leftQuantifier;
        this.rightQuantifier = rightQuantifier;
        this.joinPredicates = ImmutableList.copyOf(joinPredicates);
        this.resultValue = resultValue;
        this.aliasToQuantifierMapSupplier = Suppliers.memoize(
                () -> Quantifiers.aliasToQuantifierMap(getQuantifiers()));
    }

    /** Returns the type of outer join. */
    @Nonnull
    public JoinType getJoinType() {
        return joinType;
    }

    /** Returns the left-side quantifier. */
    @Nonnull
    public Quantifier.ForEach getLeftQuantifier() {
        return leftQuantifier;
    }

    /** Returns the right-side quantifier. */
    @Nonnull
    public Quantifier.ForEach getRightQuantifier() {
        return rightQuantifier;
    }

    /**
     * Returns the preserved-side quantifier. For a {@code LEFT} or {@code RIGHT} outer join this is the quantifier of
     * the side whose rows are never eliminated.
     *
     * @throws IllegalStateException if the join type is {@code FULL}
     */
    @Nonnull
    public Quantifier.ForEach getPreservedQuantifier() {
        return switch (joinType) {
            case LEFT -> leftQuantifier;
            case RIGHT -> rightQuantifier;
            default -> throw new IllegalStateException("FULL OUTER JOIN has no single preserved side");
        };
    }

    /**
     * Returns the null-supplying-side quantifier. This is the side whose columns are padded with {@code NULL}
     * when no match is found.
     *
     * @throws IllegalStateException if the join type is {@code FULL}
     */
    @Nonnull
    public Quantifier.ForEach getNullSupplyingQuantifier() {
        return switch (joinType) {
            case LEFT -> rightQuantifier;
            case RIGHT -> leftQuantifier;
            default -> throw new IllegalStateException("FULL OUTER JOIN has no single null-supplying side");
        };
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
     * Returns the two quantifiers, left and right, in exactly that order.
     */
    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(leftQuantifier, rightQuantifier);
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
        final Map<CorrelationIdentifier, Quantifier> aliasToQuantifierMap
                = Quantifiers.aliasToQuantifierMap(getQuantifiers());
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
        final List<QueryPredicate> translatedPredicates =
                joinPredicates.stream()
                        .map(p -> p.translateCorrelations(translationMap, shouldSimplifyValues))
                        .collect(Collectors.toList());
        final Value translatedResultValue =
                resultValue.translateCorrelations(translationMap, shouldSimplifyValues);
        return new OuterJoinExpression(
                joinType,
                translatedQuantifiers.get(0).narrow(Quantifier.ForEach.class),
                translatedQuantifiers.get(1).narrow(Quantifier.ForEach.class),
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
     * <p>Two {@code OuterJoinExpression}s are equal without children if they have the same {@link JoinType} and their
     * result values and join predicates are semantically equal under the given alias mapping.
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
        return joinType == other.joinType &&
                semanticEqualsForResults(otherExpression, aliasMap) &&
                joinPredicates.size() == other.joinPredicates.size() &&
                Streams.zip(joinPredicates.stream(),
                                other.joinPredicates.stream(),
                                (p, op) -> p.semanticEquals(op, aliasMap))
                        .allMatch(isSame -> isSame);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(joinType, joinPredicates, resultValue);
    }

    /**
     * Produces a planner-graph node labeled with the join type (e.g. "LEFT OUTER JOIN") and the {@code ON} predicate
     * as detail text.
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
                new PlannerGraph.LogicalOperatorNode(this,
                        joinType.getDisplayName(),
                        details,
                        ImmutableMap.of()),
                childGraphs);
    }
}
