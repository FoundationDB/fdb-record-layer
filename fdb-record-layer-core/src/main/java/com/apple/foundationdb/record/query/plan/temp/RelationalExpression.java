/*
 * RelationalExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A relational expression is a {@link RelationalExpression} that represents a stream of records. At all times, the root
 * expression being planned must be relational. This interface acts as a common tag interface for
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s, which can actually produce a stream of records,
 * and various logical relational expressions (not yet introduced), which represent an abstract stream of records but can't
 * be executed directly (such as an unimplemented sort). Other planner expressions such as {@link com.apple.foundationdb.record.query.expressions.QueryComponent}
 * and {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression} do not represent streams of records.
 *
 * The basic type that represents a part of the planner expression tree. An expression is generally an immutable
 * object with two different kinds of fields: regular Java fields and reference fields. The regular fields represent
 * "node information", which pertains only to this specific node in the tree. In contrast, the reference fields represent
 * this expression's children in the tree, such as its inputs and filter/sort expressions, and are always hidden behind
 * an {@link ExpressionRef}.
 *
 * Deciding whether certain fields constitute "node information" (and should therefore be a regular field) or
 * "hierarchical information" (and therefore should not be) is subtle and more of an art than a science. There are two
 * reasonable tests that can help make this decision:
 * <ol>
 *     <li>When writing a planner rule to manipulate this field, does it make sense to match it separately
 *     or access it as a getter on the matched operator? Will you ever want to match to just this field?</li>
 *     <li>Should the planner memoize (and therefore optimize) this field separately from its parent?</li>
 * </ol>
 *
 * For example, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} has only regular fields, including the
 * index name and the comparisons to use when scanning it.
 * Applying the first rule, it wouldn't really make sense to match the index name or the comparisons being performed on
 * their own: they're what define an index scan, after all!
 * Applying the second rule, they're relatively small immutable objects that don't need to be memoized.
 *
 * In contrast, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan} has no regular fields.
 * A filter plan has two important fields: the <code>Query.Component</code> used for the filter and a child plan that
 * provides input. Both of these might be matched by rules directly, in order to optimize them without regard for the
 * fact that there's a filter. Similarly, they should both be memoized separately, since there might be many possible
 * implementations of each.
 */
@API(API.Status.EXPERIMENTAL)
public interface RelationalExpression extends Bindable, Correlated<RelationalExpression> {
    @Nonnull
    static RelationalExpression fromRecordQueryOld(@Nonnull RecordQuery query, @Nonnull PlanContext context) {
        Quantifier.ForEach quantifier = Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(context.getMetaData().getRecordTypes().keySet())));
        final ViewExpression.Builder builder = ViewExpression.builder();
        for (String recordType : context.getRecordTypes()) {
            builder.addRecordType(recordType);
        }
        final Source baseSource = builder.buildBaseSource();
        if (query.getSort() != null) {
            List<Element> normalizedSort = query.getSort()
                    .normalizeForPlannerOld(baseSource, Collections.emptyList())
                    .flattenForPlannerOld();
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalSortExpression(Collections.emptyList(), normalizedSort, query.isSortReverse(), quantifier)));
        }

        if (query.getFilter() != null) {
            final QueryPredicate normalized = query.getFilter().normalizeForPlannerOld(baseSource);
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalFilterExpression(baseSource, normalized, quantifier)));
        }

        if (!query.getRecordTypes().isEmpty()) {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalTypeFilterExpression(new HashSet<>(query.getRecordTypes()), quantifier)));
        }
        if (query.removesDuplicates()) {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalDistinctExpression(quantifier)));
        }
        return Iterables.getOnlyElement(quantifier.getRangesOver().getMembers());
    }

    @Nonnull
    static RelationalExpression fromRecordQuery(@Nonnull RecordQuery query, @Nonnull PlanContext context) {
        Quantifier.ForEach quantifier = Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(context.getMetaData().getRecordTypes().keySet())));

        final ViewExpression.Builder builder = ViewExpression.builder();
        for (String recordType : context.getRecordTypes()) {
            builder.addRecordType(recordType);
        }
        final Source baseSource = builder.buildBaseSource();
        if (query.getSort() != null) {
            List<Element> normalizedSort = query.getSort()
                    .normalizeForPlannerOld(baseSource, Collections.emptyList())
                    .flattenForPlannerOld();
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalSortExpression(Collections.emptyList(), normalizedSort, query.isSortReverse(), quantifier)));
        }

        SelectExpression.Builder base = new SelectExpression.Builder(quantifier);
        if (query.getFilter() != null) {
            QueryPredicate predicate = query.getFilter().normalizeForPlanner(base);
            base.addPredicate(predicate);
        }

        if (!query.getRecordTypes().isEmpty()) {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalTypeFilterExpression(new HashSet<>(query.getRecordTypes()), quantifier)));
        }
        if (query.removesDuplicates()) {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalDistinctExpression(quantifier)));
        }
        return quantifier.getRangesOver().get();
    }

    @Nonnull
    static Set<RelationalExpression> fromIndexDefinition(@Nonnull String indexType,
                                                         @Nonnull Collection<String> recordTypes,
                                                         @Nonnull KeyExpression rootExpression) {
        Set<String> recordTypeSet = ImmutableSet.copyOf(recordTypes);
        Quantifier.ForEach quantifier = Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(recordTypeSet)));
        quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalTypeFilterExpression(recordTypeSet, quantifier)));
        if (indexType.equals(IndexTypes.VALUE)) {
            ImmutableSet.Builder<RelationalExpression> indexScanExpressions = ImmutableSet.builder();

            {
                // Completely empty scan with no comparisons at all
                SelectExpression.Builder baseBuilder = new SelectExpression.Builder(quantifier);
                rootExpression.normalizeForPlanner(baseBuilder).forEach(pred -> pred.freezeToType(ComparisonRange.Type.EMPTY));
                indexScanExpressions.add(baseBuilder.build());
            }

            // TODO add equality-only ones
            for (int inequalityIndex = 0; inequalityIndex < rootExpression.getColumnSize(); inequalityIndex++) {
                SelectExpression.Builder baseBuilder = new SelectExpression.Builder(quantifier);
                List<ValueComparisonRangePredicate> predicates = rootExpression.normalizeForPlanner(baseBuilder);
                Verify.verify(inequalityIndex == predicates.size());
                for (int ix = 0; ix < predicates.size(); ix++) {
                    ValueComparisonRangePredicate predicate = predicates.get(ix);
                    if (ix < inequalityIndex) {
                        predicate.freezeToType(ComparisonRange.Type.EQUALITY);
                    } else if (ix == inequalityIndex) {
                        predicate.freezeToType(ComparisonRange.Type.INEQUALITY);
                    } else {
                        predicate.freezeToType(ComparisonRange.Type.EMPTY);
                    }
                }
                indexScanExpressions.add(baseBuilder.build());
            }

            return indexScanExpressions.build();
        }

        return Collections.emptySet();
    }

    /**
     * Matches a matcher expression to an expression tree rooted at this node, adding to some existing bindings.
     * @param matcher the matcher to match against
     * @return the existing bindings extended with some new ones if the match was successful or <code>Optional.empty()</code> otherwise
     */
    @Override
    @Nonnull
    default Stream<PlannerBindings> bindTo(@Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        Stream<PlannerBindings> bindings = matcher.matchWith(this);
        // TODO this is probably kind of inefficient for the really common case where we don't match at all.
        return bindings.flatMap(outerBindings -> matcher.getChildrenMatcher().matches(getQuantifiers())
                .map(outerBindings::mergedWith));
    }

    /**
     * Return an iterator of references to the children of this planner expression. The iterators returned by different
     * calls are guaranteed to be independent (i.e., advancing one will not advance another). However, they might point
     * to the same object, as when <code>Collections.emptyIterator()</code> is returned. The returned iterator should
     * be treated as an immutable object and may throw an exception if {@link Iterator#remove} is called.
     * The iterator must return its elements in a consistent order.
     * @return an iterator of references to the children of this planner expression
     */
    @Nonnull
    List<? extends Quantifier> getQuantifiers();

    /**
     * Returns if this expression can be the anchor of a correlation.
     *
     * A correlation is always formed between three entities:
     * <ol>
     * <li>the {@link Quantifier} that flows data</li>
     * <li>2. the anchor (which is a {@link RelationalExpression}) that ranges directly over the source</li>
     * <li>3. the consumers (or dependents) of the correlation which must be a descendant of the anchor.</li>
     * </ol>
     *
     * In order for a correlation to be meaningful, the anchor must define how data is bound and used by all
     * dependents. For most expressions it is not meaningful or even possible to define correlation in such a way.
     *
     * For instance, a {@link com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression}
     * cannot correlate (this method returns {@code false}) because it is not meaningful to bind a record from one child
     * of the union while providing bound values to another.
     *
     * In another example, a logical select expression can correlate which means that one child of the SELECT expression
     * can be evaluated and the resulting records can bound individually one after another. For each bound record
     * flowing along that quantifier the other children of the SELECT expression can be evaluated, potentially causing
     * more correlation values to be bound, etc. These concepts follow closely to the mechanics of what SQL calls a query
     * block.
     *
     * The existence of a correlation between source, anchor, and dependents may adversely affect planning because
     * a correlation always imposes order between the evaluated of children of an expression. This may or may
     * not tie the hands of the planner to produce an optimal plan. In certain cases, queries written in a correlated
     * way can be <em>de-correlated</em> to allow for better optimization techniques.
     *
     * @return {@code true} if this expression can be the anchor of a correlation, {@code false} otherwise.
     */
    default boolean canCorrelate() {
        return false;
    }

    boolean equalsWithoutChildren(@Nonnull final RelationalExpression other,
                                  @Nonnull final AliasMap equivalences);

    int hashCodeWithoutChildren();

    /**
     * Overloaded method to call {@link #semanticEquals} with an empty alias map.
     * @param other object to compare to this expression
     * @return {@code true} if this object is semantically equal to {@code other} that is {@code this} and {@code other}
     *         produce the same result when invoked with no bindings, {@code false} otherwise.
     */
    default boolean semanticEquals(@Nullable final Object other) {
        return semanticEquals(other, AliasMap.empty());
    }

    @Override
    default boolean semanticEquals(@Nullable final Object other,
                                   @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final RelationalExpression otherExpression = (RelationalExpression)other;

        // We know this and otherExpression are of the same class. canCorrelate() needs to match as well.
        Verify.verify(canCorrelate() == otherExpression.canCorrelate());

        final Set<CorrelationIdentifier> correlatedTo = getCorrelatedTo();
        final Set<CorrelationIdentifier> otherCorrelatedTo = otherExpression.getCorrelatedTo();

        Sets.SetView<CorrelationIdentifier> unboundCorrelatedTo = Sets.difference(correlatedTo, equivalenceMap.sources());
        Sets.SetView<CorrelationIdentifier> unboundOtherCorrelatedTo = Sets.difference(otherCorrelatedTo, equivalenceMap.targets());

        final Sets.SetView<CorrelationIdentifier> commonUnbound = Sets.intersection(unboundCorrelatedTo, unboundOtherCorrelatedTo);
        final AliasMap identitiesMap = AliasMap.identitiesFor(commonUnbound);
        unboundCorrelatedTo = Sets.difference(correlatedTo, commonUnbound);
        unboundOtherCorrelatedTo = Sets.difference(otherCorrelatedTo, commonUnbound);

        final Iterable<AliasMap> boundCorrelatedReferencesIterable =
                AliasMap.empty()
                        .match(unboundCorrelatedTo,
                                alias -> ImmutableSet.of(),
                                unboundOtherCorrelatedTo,
                                otherAlias -> ImmutableSet.of(),
                                false,
                                (alias, otherAlias, nestedEquivalencesMap) -> true);

        for (final AliasMap boundCorrelatedReferencesMap : boundCorrelatedReferencesIterable) {
            final AliasMap.Builder boundEquivalenceMapBuilder = equivalenceMap.derived();

            boundEquivalenceMapBuilder.putAll(identitiesMap);
            boundEquivalenceMapBuilder.putAll(boundCorrelatedReferencesMap);

            final Iterable<AliasMap> aliasMapIterable =
                    Quantifiers.match(getQuantifiers(),
                            otherExpression.getQuantifiers(),
                            canCorrelate(),
                            boundEquivalenceMapBuilder.build(),
                            (quantifier, otherQuantifier, nestedEquivalenceMap) -> {
                                if (quantifier.semanticHashCode() != otherQuantifier.semanticHashCode()) {
                                    return false;
                                }
                                return quantifier.semanticEquals(otherQuantifier, nestedEquivalenceMap);
                            });

            if (StreamSupport.stream(aliasMapIterable.spliterator(), false)
                    .anyMatch(aliasMap -> equalsWithoutChildren(otherExpression, equivalenceMap.compose(aliasMap)))) {
                return true;
            }
        }

        return false;
    }

    @Override
    default int semanticHashCode() {
        return Objects.hash(getQuantifiers()
                        .stream()
                        .map(Quantifier::semanticHashCode)
                        .collect(ImmutableSet.toImmutableSet()),
                hashCodeWithoutChildren());
    }

    /**
     * Apply the given property visitor to this planner expression and its children. Returns {@code null} if
     * {@link PlannerProperty#shouldVisit(RelationalExpression)} called on this expression returns {@code false}.
     * @param visitor a {@link PlannerProperty} visitor to evaluate
     * @param <U> the type of the evaluated property
     * @return the result of evaluating the property on the subtree rooted at this expression
     */
    @Nullable
    default <U> U acceptPropertyVisitor(@Nonnull PlannerProperty<U> visitor) {
        if (visitor.shouldVisit(this)) {
            final List<U> quantifierResults = new ArrayList<>();
            final List<? extends Quantifier> quantifiers = getQuantifiers();
            for (final Quantifier quantifier : quantifiers) {
                quantifierResults.add(quantifier.acceptPropertyVisitor(visitor));
            }

            return visitor.evaluateAtExpression(this, quantifierResults);
        }
        return null;
    }

    /**
     * This is needed for graph integration into IntelliJ as IntelliJ only ever evaluates selfish methods. Add this
     * method as a custom renderer for the type {@link RelationalExpression}. During debugging you can then for instance
     * click show() on an instance and enjoy the query graph it represents rendered in your standard browser.
     * @param renderSingleGroups whether to render group references with just one member
     * @return the String "Done."
     */
    @Nonnull
    default String show(final boolean renderSingleGroups) {
        return PlannerGraphProperty.show(renderSingleGroups, this);
    }
}
