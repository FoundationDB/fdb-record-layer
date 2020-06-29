/*
 * InExtractor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.ComponentWithChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.ComponentWithNoChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlanOrderingKey;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Planner helper to facilitate
 * 1. IN to JOIN transformation: {@code FROM sub WHERE f IN (v1, ..., vn)} to
 *    {@code VALUES(v1, ..., vn) AS v JOIN (SELECT FROM sub WHERE f = v)} transformations as well as
 * 2. IN to OR transformation: {@code FROM sub WHERE f IN (v1, ..., vn)} to
 *    {@code FROM sub WHERE f = v1 OR f = v2 OR ... f = vn} transformations.
 *
 * An object of this class is used during the planning of a query to represent all IN clauses of the query
 * being planned. A specified IN clause may be considered for an IN to JOIN transformation iff
 * 1. a required ordering of the query is not destroyed by that transformation and
 * 2. the IN JOIN transformation utilizes an index because of the transformed predicate
 *
 * As 1. is a statically computed property; it represents the super list of all the INs we can potentially transform
 * to JOINs. 2. is plan-dependent and therefore needs to be re-established on a plan-by-plan basis. That means that a particular
 * IN to JOIN transformation may not be doable for the given plan. An additional complicating factor in this case is
 * that if an order of the resulting query is required, it is not possible to perform IN to JOIN transformations for
 * any subsequent INs either.
 */
@API(API.Status.INTERNAL)
public class InExtractor {

    /**
     * The original filter. All IN (...) clauses are expressed as {@link FieldWithComparison} inside this
     * {@link QueryComponent}.
     */
    @Nonnull
    private final QueryComponent filter;

    /**
     * Indicator if the planner has pushed ordering requirements into the extractor. Note, it can happen that
     * {@link #orderInfoNeeded()} morphs the contained {@link InClause} to become sorted, however, that is just
     * used to establish an order at all.
     * TODO: maybe this should always be on
     */
    private final boolean hasOrderingRequirements;

    /**
     * All IN clauses that can potentially transformed into a nested loop join. Ideally we want to extract and
     * transform all IN clauses, however, it may either become necessary to NOT transform some of them as
     * required semantics of the original query cannot be preserved otherwise. This field is not plan-dependent.
     */
    @Nonnull
    private final List<InClause> inClauses;

    /**
     * A different version of {@code filter} that substitutes all IN clauses that are semantically possible, i.e.
     * the ones in {@code inClauses}, by a correlated equality filter. This is the filter the planner uses
     * to plan the inner of the query (the access path).
     */
    private final QueryComponent subFilter;

    /**
     * Overloaded constructor. This constructor should only every be called from static factory methods residing within
     * this class and is therefore {@code private}.
     * @param filter -- the filter
     * @param hasOrderingRequirements -- indicator whether we need to enforce ordering requirements
     * @param inClauses -- the in clauses
     * @param subFilter -- the sub filter
     */
    private InExtractor(@Nonnull QueryComponent filter,
                        final boolean hasOrderingRequirements,
                        @Nonnull List<InClause> inClauses,
                        @Nonnull QueryComponent subFilter) {
        this.filter = filter;
        this.hasOrderingRequirements = hasOrderingRequirements;
        this.inClauses = ImmutableList.copyOf(inClauses);
        this.subFilter = subFilter;
    }

    /**
     * Getter to return if there are IN clauses at all in this filter component.
     * @return {@code true} if there are IN clauses in this filter component, {@code false} otherwise
     */
    @Nonnull
    public boolean hasInClauses() {
        return !inClauses.isEmpty();
    }

    /**
     * Getter to return the filter that then is substituted by the planner for planning of the inner.
     * @return the sub filter
     */
    @Nonnull
    public QueryComponent getSubFilter() {
        return subFilter;
    }

    /**
     * Method to create a new {@code InExtractor} based on this {@code InExtractor} using a list
     * of ordering requirements synthesized from a {@link KeyExpression}.
     * @param key a key expression defining the the ordering requirements
     * @param reverse an indicator of whether we should order in the reverse way
     * @return a new {@code InExtractor} using the given ordering requirements.
     */
    @Nonnull
    public InExtractor withOrderingRequirements(@Nonnull final KeyExpression key, final boolean reverse) {
        if (inClauses.isEmpty()) {
            return this;
        }

        final List<KeyExpression> sortComponents = key.normalizeKeyForPositions();
        final List<InClause> adjustedInClauses = new ArrayList<>(inClauses);

        int i = 0;
        while (i < sortComponents.size() && i < adjustedInClauses.size()) {
            final KeyExpression sortComponent = sortComponents.get(i);
            boolean found = false;
            for (int j = i; j < adjustedInClauses.size(); j++) {
                final InClause inClause = adjustedInClauses.get(j);
                if (sortComponent.equals(inClause.orderingKey)) {
                    if (i != j) {
                        adjustedInClauses.remove(j);
                        adjustedInClauses.add(i, inClause.sort(reverse));
                    } else {
                        adjustedInClauses.set(i, inClause.sort(reverse));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                // There is a requested sort ahead of the ones from the IN's, so we can't go on from here.
                adjustedInClauses.subList(i, adjustedInClauses.size()).clear();
                break;
            }
            i++;
        }
        return fromFilterWithConstraints(filter, true, adjustedInClauses);
    }

    /**
     * Method to create a new {@code InExtractor} based on this {@code InExtractor} that causes all of its
     * in clauses to be sorted.
     * @return a new {@code InExtractor}
     */
    @Nonnull
    public InExtractor orderInfoNeeded() {
        return new InExtractor(filter, false, inClauses.stream().map(InClause::sort).collect(Collectors.toList()), subFilter);
    }

    /**
     * Method to create a new {@code InExtractor} based on this {@code InExtractor} that explicitly does not extract
     * and transform any {@code IN (...)} clauses. A query that is planned with the resulting {@code InExtractor} does
     * not just not cause any query transformations, it implicitly also does not interfere with the order of the inner
     * plan. Note that while not deprecated this method provides a rather heavy-handed approach to disable all IN to
     * JOIN transformations. Currently, it is necessary to do either (IN / IN to JOIN transformation) OR (IN to OR)
     * but not a mix. Therefore, if planning of IN to JOIN does not produce a viable plan we revert and try again as
     * IN to OR if warranted.
     * @return a new {@code InExtractor} that does not consider any {@code IN(...)} clauses
     */
    @Nonnull
    public InExtractor cancel() {
        return fromFilterWithConstraints(filter, false, ImmutableList.of());
    }

    /**
     * Method to create a new {@link PlanDependent} based on this {@code InExtractor} that uses a list of
     * unsatisfiable filters ({@link QueryComponent}s) to partition all IN clauses known to this {@code InExtractor}
     * into a list of sargable INs (the ones that can be handled in index scans, sargable for SearchARGumentABLE) and
     * a list of residual INs (the ones that have to be applied as separate filters). We do not want to do the
     * IN to JOIN transformation for residual INs.
     * @param unsatisfiedFilters a list of unsatisfied filters that could not be appliead as index search arguments
     * @return a new {@link InExtractor.PlanDependent} that combines this {@code InExtractor} with plan-dependent
     *         information (in particular computation of sargable and residual INs).
     */
    @Nonnull
    public PlanDependent withUnsatisfiedFilters(@Nonnull final List<QueryComponent> unsatisfiedFilters) {
        final BiMap<String, InClause> bindingToInClauseMap = getBindingToInClauseMap(inClauses);
        final ImmutableBiMap<String, InClause> unsatisfiedBindingNameToInClauseMap = unsatisfiedFilters
                .stream()
                .flatMap(component -> correlatedToInClauses(component, bindingToInClauseMap).stream())
                .collect(ImmutableBiMap.toImmutableBiMap(inClause -> inClause.bindingName, inClause -> inClause));

        return new PlanDependent(getSargableInClauses(unsatisfiedBindingNameToInClauseMap));
    }

    /**
     * Method to create a new {@link PlanDependent} based on this {@code InExtractor} that treats all INs as residual
     * INs. In some sense this method is the logical counterpart to {@link #cancel()}, however, this method creates
     * a {@link PlanDependent} instead. Using the resulting {@code PlanDependent} causes the planner to NOT apply
     * any IN to JOIN list transformations. This method is used to plan the default path where no indexes are used
     * at all and all INs should be treated as residual INs.
     * @return a new {@link InExtractor.PlanDependent} that combines this {@code InExtractor} with plan-dependent
     *         information (in particular computation of sargable and residual INs).
     */
    @Nonnull
    public PlanDependent withResidualInClauses() {
        return new PlanDependent(ImmutableList.of());
    }

    /**
     * Compute a list of sargable (SearchARGumentABLE) IN clauses that can be used for IN to JOIN transformations using a
     * map of binding -> inClause that contains mappings to in clauses that could not be satisfied by the inner plan.
     *
     * Conversely, in some sense this method returns the inverse of the unsatisfied bindings, i.e. the satisfied
     * bindings. However, there are some details to this: We traverse the IN clauses known to this {@code InExtractor}
     * in order. Depending on ordering requirements we cannot just filter out the IN clauses that are contained in
     * unsatisfied filters. If we encounter an IN that is referred to in an unsatisfied filter AND we need to enforce
     * ordering requirements we abort the traversal and only treat the already processed IN clauses as sargables.
     *
     * @param unsatisfiedBindingNameToInClauseMap the map containing unsatisfied binding -> inClause
     * @return a list of sargable IN clauses
     */
    @Nonnull
    private List<InClause> getSargableInClauses(@Nonnull final BiMap<String, InClause> unsatisfiedBindingNameToInClauseMap) {
        final ImmutableList.Builder<InClause> sargableInClausesBuilder = ImmutableList.builder();
        for (final InClause inClause : inClauses) {
            if (unsatisfiedBindingNameToInClauseMap.containsValue(inClause)) {
                if (hasOrderingRequirements) {
                    break;
                }  else {
                    continue;
                }
            }
            sargableInClausesBuilder.add(inClause);
        }

        return sargableInClausesBuilder.build();
    }

    /**
     * Transforms all INs in the {@code filter} into OR disjuncts.
     * @return a new {@link QueryComponent} where all INs are replaced by OR constructs.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public QueryComponent asOr() {
        return mapClauses(filter, (withComparison, fields) -> {
            if (withComparison.getComparison().getType() == Comparisons.Type.IN) {
                if (withComparison.getComparison() instanceof Comparisons.ParameterComparison) {
                    return withComparison;
                } else {
                    final List<Object> comparands = (List<Object>)Objects.requireNonNull(withComparison.getComparison().getComparand());
                    final List<QueryComponent> orBranches = new ArrayList<>();
                    for (final Object comparand : comparands) {
                        orBranches.add(withComparison.withOtherComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand)));
                    }

                    // OR must have at least two branches.
                    if (orBranches.size() == 1) {
                        return orBranches.get(0);
                    } else {
                        return Query.or(orBranches);
                    }
                }
            } else {
                return withComparison;
            }

        }, Collections.emptyList());
    }

    /**
     * Factory method to create a new {@code InExtractor} using a given filter.
     * @param filter the filter
     * @return a new {@code InExtractor}
     */
    @Nonnull
    public static InExtractor fromFilter(@Nonnull QueryComponent filter) {
        return fromFilterWithConstraints(filter, false, null);
    }

    /**
     * Factory method to create a new {@code InExtractor} using a given filter and a list of IN clauses that function
     * as a constraint for the creating of the new {@code InExtractor}. The new {@code InExtractor} can only refer
     * to at most the {@link InClause}s contained in the constraints handed in.
     * @param filter the filter
     * @param hasOrderingRequirments indicator if the query has ordering requirements
     * @param inClauseConstraints a list of IN clauses that represents a superset of IN clauses this {@code InExtractor}
     *        can use. If {@code null}, then this methods assumes no constraints need to be applied.
     * @return a new {@code InExtractor}
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static InExtractor fromFilterWithConstraints(@Nonnull QueryComponent filter,
                                                        final boolean hasOrderingRequirments,
                                                        @Nullable final List<InClause> inClauseConstraints) {
        final AtomicInteger bindingIndex = new AtomicInteger();
        @Nullable final Set<String> fieldNameConstraints =
                inClauseConstraints == null
                ? null
                : inClauseConstraints.stream().map(inClause -> inClause.fieldName).collect(ImmutableSet.toImmutableSet());

        final ImmutableList.Builder<InClause> inClausesBuilder = ImmutableList.builder();
        final QueryComponent subFilter = mapClauses(filter, (withComparison, fields) -> {
            if (withComparison.getComparison().getType() == Comparisons.Type.IN) {
                final String fieldName = withComparison.getName();
                if (fieldNameConstraints == null || fieldNameConstraints.contains(fieldName)) {
                    String bindingName = Bindings.Internal.IN.bindingName(
                            fieldName + "__" + bindingIndex.getAndIncrement());
                    List<FieldKeyExpression> nestedFields = null;
                    if (fields != null && withComparison instanceof FieldWithComparison) {
                        nestedFields = new ArrayList<>(fields);
                        nestedFields.add(Key.Expressions.field(((FieldWithComparison)withComparison).getFieldName()));
                    }
                    KeyExpression orderingKey = getOrderingKey(nestedFields);

                    if (withComparison.getComparison() instanceof Comparisons.ParameterComparison) {
                        final String parameterName = ((Comparisons.ParameterComparison)withComparison.getComparison()).getParameter();
                        inClausesBuilder.add(new InParameterClause(fieldName, bindingName, orderingKey, false, false, parameterName));
                    } else {
                        final List<Object> comparand = (List<Object>)Objects.requireNonNull(withComparison.getComparison().getComparand());
                        // ListComparison does not allow empty/null
                        if (comparand.size() == 1) {
                            return withComparison.withOtherComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand.get(0)));
                        }
                        inClausesBuilder.add(new InValuesClause(fieldName, bindingName, orderingKey, false, false, comparand));
                    }
                    return withComparison.withOtherComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, bindingName, Bindings.Internal.IN));
                }
            }
            return withComparison;
        }, Collections.emptyList());

        final ImmutableList<InClause> inClauses = inClausesBuilder.build();
        if (inClauseConstraints == null) {
            return new InExtractor(filter, hasOrderingRequirments, inClauses, subFilter);
        } else {
            Verify.verify(inClauseConstraints.size() == inClauses.size());
            return new InExtractor(filter, hasOrderingRequirments, inClauseConstraints, subFilter);
        }
    }

    /**
     * Map the clauses contained in the given {@code filter}. This method transforms one {@link QueryComponent} into
     * a new {@link QueryComponent} by using a given lambda to perform substitutions.
     * @param filter the filter to map over
     * @param mapper bifunction to perform substitutions
     * @param fields list of {@link FieldKeyExpression}s to keep track of field nesting
     * @return a new {@link QueryComponent} with substituted sub-components
     */
    @Nonnull
    private static QueryComponent mapClauses(@Nonnull final QueryComponent filter,
                                             @Nonnull BiFunction<ComponentWithComparison, List<FieldKeyExpression>, QueryComponent> mapper,
                                             @Nullable List<FieldKeyExpression> fields) {
        if (filter instanceof ComponentWithComparison) {
            final ComponentWithComparison withComparison = (ComponentWithComparison) filter;
            return mapper.apply(withComparison, fields);
        } else if (filter instanceof ComponentWithChildren) {
            ComponentWithChildren componentWithChildren = (ComponentWithChildren) filter;
            return componentWithChildren.withOtherChildren(
                    componentWithChildren.getChildren().stream()
                            .map(component -> mapClauses(component, mapper, fields))
                            .collect(Collectors.toList())
            );
        } else if (filter instanceof ComponentWithSingleChild) {
            ComponentWithSingleChild componentWithSingleChild = (ComponentWithSingleChild) filter;
            List<FieldKeyExpression> nestedFields = null;
            if (fields != null && componentWithSingleChild instanceof NestedField) {
                nestedFields = new ArrayList<>(fields);
                nestedFields.add(Key.Expressions.field(((NestedField) componentWithSingleChild).getFieldName()));
            }
            return componentWithSingleChild.withOtherChild(
                    mapClauses(componentWithSingleChild.getChild(), mapper, nestedFields));
        } else if (filter instanceof ComponentWithNoChildren) {
            return filter;
        } else {
            throw new Query.InvalidExpressionException("Unsupported query type " + filter.getClass());
        }
    }

    @Nullable
    private static KeyExpression getOrderingKey(List<FieldKeyExpression> fields) {
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        KeyExpression key = fields.get(fields.size() - 1);
        for (int i = fields.size() - 2; i >= 0; i--) {
            key = new NestingKeyExpression(fields.get(i), key);
        }
        return key;
    }

    @Nonnull
    private static BiMap<String, InClause> getBindingToInClauseMap(final List<InClause> inClauses) {
        return inClauses.stream()
                .collect(ImmutableBiMap.toImmutableBiMap(inClause -> inClause.bindingName, inClause -> inClause));
    }

    @Nonnull
    private static Set<InClause> correlatedToInClauses(@Nonnull final QueryComponent component, @Nonnull Map<String, InClause> bindingToInClauseMap) {
        if (component instanceof FieldWithComparison) {
            final FieldWithComparison fieldWithComparison = (FieldWithComparison)component;
            final Comparisons.Comparison comparison = fieldWithComparison.getComparison();
            if (comparison.getType() == Comparisons.Type.EQUALS && comparison instanceof Comparisons.ParameterComparison) {
                final String parameterName = ((Comparisons.ParameterComparison)comparison).getParameter();
                return bindingToInClauseMap.containsKey(parameterName) ? ImmutableSet.of(bindingToInClauseMap.get(parameterName)) : ImmutableSet.of();
            }
            return ImmutableSet.of();
        }

        if (component instanceof ComponentWithChildren) {
            final List<QueryComponent> children = ((ComponentWithChildren)component).getChildren();
            return children.stream()
                    .flatMap(child -> correlatedToInClauses(child, bindingToInClauseMap).stream())
                    .collect(ImmutableSet.toImmutableSet());
        }

        return ImmutableSet.of();
    }

    /**
     * Base class for all IN clauses.
     */
    abstract static class InClause {
        /**
         * field Name of the IN clause as in {@code fieldName IN (1, 2, 3)}.
         */
        @Nonnull
        protected final String fieldName;

        /**
         * The correlation/ binding name this IN clause uses in its transformed JOIN form which, i.e.:
         * {@code fieldName = bindingName};
         * where {@code bindingName} is defined in the outer that is joined to the planned inner
         */
        @Nonnull
        protected final String bindingName;

        /**
         * Optional ordering key if needed.
         */
        @Nullable
        protected final KeyExpression orderingKey;

        /**
         * Indicator if the values that are correlated into the sub-plan should be explicitly sorted.
         */
        protected final boolean sortValues;

        /**
         * Indicator if the values that are correlated into the sub-plan should be explicitly reversed.
         * This flag has no meaning in the absence of {@link #sortValues}.
         */
        protected final boolean sortReverse;

        /**
         * Overloaded protected constructor.
         * @param fieldName field name for the clause
         * @param bindingName binding name for the clause that is used if IN to JOIN transformation is applied
         * @param orderingKey ordering key (optional)
         * @param sortValues indicator whether values should be sorted if IN to JOIN transformation is applied
         * @param sortReverse indicator whether values should be sorted reversed if IN to JOIN transformation is
         *                    applied
         */
        protected InClause(@Nonnull final String fieldName,
                           @Nonnull final String bindingName,
                           @Nullable final KeyExpression orderingKey,
                           final boolean sortValues,
                           final boolean sortReverse) {
            this.fieldName = fieldName;
            this.bindingName = bindingName;
            this.orderingKey = orderingKey;
            this.sortValues = sortValues;
            this.sortReverse = sortReverse;
        }

        @Nonnull
        protected InClause sort() {
            return sort(sortReverse);
        }

        /**
         * Create a new sorted {@code InClause} based on the this {@code InClause}.
         * @param sortReverse sort reversed
         * @return a new {@code InClause}
         */
        @Nonnull
        protected abstract InClause sort(boolean sortReverse);

        /**
         * Create a new {@link QueryComponent} of this IN clause in its original form, i.e {@code fieldName IN ()}
         * @return a new {@link QueryComponent}
         */
        @Nonnull
        protected abstract QueryComponent toInFilter();

        /**
         * Wrap a {@link RecordQueryInParameterJoinPlan} around the inner plan for this {@code InClause}.
         * @param inner the inner plan
         * @return a new {@link RecordQueryInJoinPlan} that is equivalent to {@code NLJN(VALUES(...) as v, inner)} where
         *         inner uses {@code v} in a sarged filter
         */
        @Nonnull
        protected abstract RecordQueryInJoinPlan wrap(@Nonnull RecordQueryPlan inner);
    }

    /**
     * Class for all VALUES()-based IN clauses.
     */
    static class InValuesClause extends InClause {
        @Nonnull
        private final List<Object> values;

        protected InValuesClause(@Nonnull final String fieldName,
                                 @Nonnull final String bindingName,
                                 @Nullable final KeyExpression orderingKey,
                                 final boolean sortValues,
                                 final boolean sortReverse,
                                 @Nonnull final List<Object> values) {
            super(fieldName, bindingName, orderingKey, sortValues, sortReverse);
            this.values = values;
        }

        @Nonnull
        @Override
        protected InValuesClause sort(final boolean sortReverse) {
            return new InValuesClause(fieldName, bindingName, orderingKey, true, sortReverse, values);
        }

        @Nonnull
        @Override
        protected QueryComponent toInFilter() {
            return Query.field(fieldName).in(values);
        }

        @Nonnull
        @Override
        protected RecordQueryInValuesJoinPlan wrap(@Nonnull final RecordQueryPlan inner) {
            return new RecordQueryInValuesJoinPlan(inner, bindingName, values, sortValues, sortReverse);
        }
    }

    /**
     * Class for all parameter marker-based IN clauses.
     */
    static class InParameterClause extends InClause {
        @Nonnull
        private final String parameterName;

        protected InParameterClause(@Nonnull final String fieldName,
                                    @Nonnull final String bindingName,
                                    @Nullable final KeyExpression orderingKey,
                                    final boolean sortValues,
                                    final boolean sortReverse,
                                    @Nonnull final String parameterName) {
            super(fieldName, bindingName, orderingKey, sortValues, sortReverse);
            this.parameterName = parameterName;
        }

        @Nonnull
        @Override
        protected InParameterClause sort(final boolean sortReverse) {
            return new InParameterClause(fieldName, bindingName, orderingKey, true, sortReverse, parameterName);
        }

        @Nonnull
        @Override
        protected QueryComponent toInFilter() {
            return Query.field(fieldName).in(parameterName);
        }

        @Nonnull
        @Override
        protected RecordQueryInParameterJoinPlan wrap(@Nonnull final RecordQueryPlan inner) {
            return new RecordQueryInParameterJoinPlan(inner, bindingName, parameterName, sortValues, sortReverse);
        }
    }

    /**
     * This class represents a plan-dependent or plan-specific version of its outer class {@link InExtractor}.
     * After planning an inner using the sub-filter we may determine that the substituted correlated filters are not
     * used as index search arguments which would make these IN clauses residual IN clauses. Even though correct we
     * should not do the IN to JOIN transformation for these IN clauses as the resulting plan would rescan the same
     * inner mulitple times and only subsequently filter using the unsatisfied filters determined during the planning
     * of the inner.
     * In a sense this class act like a view of {@code InExtractor} where some {@link #inClauses} are transformed while
     * some others are not transformed into JOINs entirely based on the information held by objects of this inner class.
     */
    public class PlanDependent {
        /**
         * All SearchARGumentABLE IN clauses. This is a subset of all {@link #inClauses} in {@code InExtractor.this}.
         */
        @Nonnull
        private final List<InClause> sargableInClauses;

        /**
         * All residual in clauses. This is a subset of all {@link #inClauses} in {@code InExtractor.this}.
         * Also this is the exact complement of {@link #sargableInClauses} with respect to all {@link #inClauses} in
         * {@code InExtractor.this}.
         */
        @Nonnull
        private final BiMap<String, InClause> residualNameToInClauseMap;

        /**
         * Private overloaded constructor.
         * @param sargableInClauses a list of sargables that we use to determine which clauses to transform into JOINs
         *        and which ones to transform into regular predicates
         */
        private PlanDependent(@Nonnull final List<InClause> sargableInClauses) {
            this.sargableInClauses = ImmutableList.copyOf(sargableInClauses);

            final ImmutableBiMap<String, InClause> sargableNameToInClauseMap = sargableInClauses
                    .stream()
                    .collect(ImmutableBiMap.toImmutableBiMap(inClause -> inClause.bindingName, inClause -> inClause));

            this.residualNameToInClauseMap =
                    inClauses.stream()
                            .filter(inClause -> !sargableNameToInClauseMap.containsKey(inClause.bindingName))
                            .collect(ImmutableBiMap.toImmutableBiMap(inClause -> inClause.bindingName, inClause -> inClause));
        }

        /**
         * Returns the number of sargable in clauses. This is used as an indicator of the quality of the inner plan
         * this this {@code PLanDependent} was created with.
         * @return the number of sargable IN clauses
         */
        public int getNumSargableInClauses() {
            return sargableInClauses.size();
        }

        /**
         * Given a list of filters (as {@link QueryComponent}s) transform their constituent parts to either use
         * {@code fieldName = bindingName} to refer to a sargable IN clause binding (already in that form) or to use
         * "fieldName IN (...)" for a residual IN-clause.
         * @param filters filters to be transformed
         * @return a new list of {@link QueryComponent}s
         */
        @Nonnull
        public List<QueryComponent> compensateWithInFilters(@Nonnull final List<QueryComponent> filters) {
            return filters.stream()
                    .map(this::compensateWithInFilters)
                    .collect(ImmutableList.toImmutableList());
        }

        /**
         * Given a filter (as {@link QueryComponent}) transform its constituent parts to either use
         * {@code fieldName = bindingName} to refer to a sargable IN clause binding (already in that form) or to use
         * "fieldName IN (...)" for a residual IN-clause.
         * @param filter filter to be transformed
         * @return a new {@link QueryComponent}s
         */
        @Nonnull
        private QueryComponent compensateWithInFilters(@Nonnull final QueryComponent filter) {
            return mapClauses(filter, (withComparison, fields) -> {
                final Comparisons.Comparison comparison = withComparison.getComparison();
                if (comparison.getType() == Comparisons.Type.EQUALS) {
                    if (comparison instanceof Comparisons.ParameterComparison) {
                        final String bindingName = ((Comparisons.ParameterComparison)comparison).getParameter();

                        if (residualNameToInClauseMap.containsKey(bindingName)) {
                            return residualNameToInClauseMap.get(bindingName).toInFilter();
                        }
                    }
                }
                return withComparison;
            }, Collections.emptyList());
        }

        /**
         * Wrap an inner plan with a {@link RecordQueryInJoinPlan} for each sargable IN-clause in reverse order.
         * @param plan the plan to be wrapper, i.e. the inner plan
         * @return a new top of a chain of {@link RecordQueryInJoinPlan} where each {@link RecordQueryInJoinPlan}
         *         processes a sargable IN-clause. Note that it is possible that the plan handed in to this method
         *         is just returned as is (specifically if there are no sargable IN-clauses).
         */
        @Nonnull
        public RecordQueryPlan wrap(@Nonnull RecordQueryPlan plan) {
            for (int i = inClauses.size() - 1; i >= 0; i--) {
                final InClause inClause = inClauses.get(i);
                if (!residualNameToInClauseMap.containsValue(inClause)) {
                    plan = inClause.wrap(plan);
                }
            }
            return plan;
        }

        /**
         * Adjusts the ordering of the inner to represent the proper ordering after IN to JOIN transformations have been
         * applied.
         * @param ordering the inherent produced ordering obtained from the inner plan
         * @return an adjusted new {@link PlanOrderingKey} representing the produced order after the IN to JOIN
         *         transformations.
         */
        @Nullable
        public PlanOrderingKey adjustOrdering(@Nullable PlanOrderingKey ordering) {
            if (ordering == null || sargableInClauses.isEmpty()) {
                return ordering;
            }
            // All the ordering keys from the IN joins look like non-prefix ordering and come before the others.
            final List<KeyExpression> keys = new ArrayList<>(ordering.getKeys());
            int prefixSize = ordering.getPrefixSize();
            final int primaryKeyStart = ordering.getPrimaryKeyStart();
            final int primaryKeyTailFromEnd = keys.size() - ordering.getPrimaryKeyTail();
            for (int i = 0; i < sargableInClauses.size(); i++) {
                final KeyExpression inOrdering = sargableInClauses.get(i).orderingKey;
                if (inOrdering == null) {
                    return null;
                }
                final int position = keys.indexOf(inOrdering);
                if (position >= 0) {
                    if (position < prefixSize) {
                        prefixSize--;   // No longer an equality.
                    }
                    keys.remove(position);
                }
                keys.add(prefixSize + i, inOrdering);
            }
            return new PlanOrderingKey(keys, prefixSize, primaryKeyStart, keys.size() - primaryKeyTailFromEnd);
        }
    }
}
