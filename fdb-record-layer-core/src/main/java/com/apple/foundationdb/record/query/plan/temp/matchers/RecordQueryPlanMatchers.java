/*
 * RecordQueryPlanMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.temp.matchers.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.ofTypeOwning;
import static com.apple.foundationdb.record.query.plan.temp.matchers.SetMatcher.exactlyInAnyOrder;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for descendants of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class RecordQueryPlanMatchers {
    private RecordQueryPlanMatchers() {
        // do not instantiate
    }

    public static BindingMatcher<RecordQueryPlan> anyPlan() {
        return RelationalExpressionMatchers.ofType(RecordQueryPlan.class);
    }

    @Nonnull
    public static <R extends RecordQueryPlan> BindingMatcher<R> childrenPlans(@Nonnull final Class<R> bindableClass, @Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of((R recordQueryPlan) -> {
                    final List<? extends Quantifier> quantifiers = recordQueryPlan.getQuantifiers();
                    final List<Iterable<RelationalExpression>>
                            rangedOverPlans = quantifiers.stream()
                            .map(quantifier -> quantifier.getRangesOver().getMembers().stream().map(r -> (RelationalExpression)r).collect(ImmutableList.toImmutableList()))
                            .collect(ImmutableList.toImmutableList());
                    return CrossProduct.crossProduct(rangedOverPlans);
                }, name -> "planChildren(" + name + ")"),
                AnyMatcher.anyInIterable(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> descendantPlans(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(plan.collectDescendantPlans()), name -> "descendantPlans(" + name + ")"),
                any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> descendantPlans(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(plan.collectDescendantPlans()), name -> "descendantPlans(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> selfOrDescendantPlans(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(Iterables.concat(plan.collectDescendantPlans(), ImmutableList.of(plan))), name -> "selfOrDescendantPlans(" + name + ")"),
                any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> selfOrDescendantPlans(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(Iterables.concat(plan.collectDescendantPlans(), ImmutableList.of(plan))), name -> "selfOrDescendantPlans(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static ListMatcher<? extends RecordQueryPlan> exactlyPlans(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return exactly(Arrays.asList(downstreams));
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static SetMatcher<? extends RecordQueryPlan> exactlyPlansInAnyOrder(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return exactlyInAnyOrder(Arrays.asList(downstreams));
    }

    public static SetMatcher<? extends RecordQueryPlan> exactlyPlansInAnyOrder(@Nonnull final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return exactlyInAnyOrder(downstreams);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryFilterPlan> filter(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFilterPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryFilterPlan> filterPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFilterPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryFilterPlan> filterPlan(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFilterPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryFilterPlan> queryComponents(@Nonnull CollectionMatcher<? extends QueryComponent> downstream) {
        return typedWithDownstream(RecordQueryFilterPlan.class,
                Extractor.of(RecordQueryFilterPlan::getFilters, name -> "filters(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryIndexPlan> indexPlan() {
        return ofTypeOwning(RecordQueryIndexPlan.class, CollectionMatcher.empty());
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInJoinPlan> inJoin(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInJoinPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInJoinPlan> inJoinPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInJoinPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInJoinPlan> inJoinPlan(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInJoinPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameterJoin(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInParameterJoinPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameterJoinPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInParameterJoinPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameterJoinPlan(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInParameterJoinPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameter(@Nonnull BindingMatcher<String> downstream) {
        return typedWithDownstream(RecordQueryInParameterJoinPlan.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getExternalBinding()), name -> "externalBinding(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesJoin(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInValuesJoinPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesJoinPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInValuesJoinPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesJoinPlan(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInValuesJoinPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesList(@Nonnull BindingMatcher<? extends Collection<?>> downstream) {
        return typedWithDownstream(RecordQueryInValuesJoinPlan.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getInListValues()), name -> "values(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlanWithComparisons> scanComparisons(@Nonnull BindingMatcher<ScanComparisons> scanComparisonsBindingMatcher) {
        return typedWithDownstream(RecordQueryPlanWithComparisons.class,
                Extractor.of(RecordQueryPlanWithComparisons::getComparisons, name -> "comparisons(" + name + ")"),
                scanComparisonsBindingMatcher);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlanWithIndex> indexName(@Nonnull String indexName) {
        return typedWithDownstream(RecordQueryPlanWithIndex.class,
                Extractor.of(RecordQueryPlanWithIndex::getIndexName, name -> "indexName(" + name + ")"),
                PrimitiveMatchers.equalsObject(indexName));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlanWithIndex> indexScanType(@Nonnull IndexScanType scanType) {
        return typedWithDownstream(RecordQueryPlanWithIndex.class,
                Extractor.of(RecordQueryPlanWithIndex::getScanType, name -> "indexScanType(" + name + ")"),
                PrimitiveMatchers.equalsObject(scanType));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryPredicatesFilterPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryPredicatesFilterPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(@Nonnull final BindingMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                   @Nonnull final BindingMatcher<? extends Quantifier> downstreamQuantifiers) {
        return RelationalExpressionMatchers.ofTypeWithPredicatesAndOwning(RecordQueryPredicatesFilterPlan.class, any(downstreamPredicates), any(downstreamQuantifiers));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(@Nonnull final CollectionMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                   @Nonnull final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return RelationalExpressionMatchers.ofTypeWithPredicatesAndOwning(RecordQueryPredicatesFilterPlan.class, downstreamPredicates, downstreamQuantifiers);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilterPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryPredicatesFilterPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilterPlan(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryPredicatesFilterPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicates(@Nonnull CollectionMatcher<? extends QueryPredicate> downstream) {
        return typedWithDownstream(RecordQueryPredicatesFilterPlan.class,
                Extractor.of(RecordQueryPredicatesFilterPlan::getPredicates, name -> "predicates(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicates(@Nonnull BindingMatcher<? extends QueryPredicate>... downstreams) {
        return typedWithDownstream(RecordQueryPredicatesFilterPlan.class,
                Extractor.of(RecordQueryPredicatesFilterPlan::getPredicates, name -> "predicates(" + name + ")"),
                exactlyInAnyOrder(downstreams));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryScanPlan> scanPlan() {
        return ofTypeOwning(RecordQueryScanPlan.class, CollectionMatcher.empty());
    }

    @Nonnull
    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilter(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryTypeFilterPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilter(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryTypeFilterPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilterPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryTypeFilterPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilterPlan(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryTypeFilterPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryTypeFilterPlan> recordTypes(@Nonnull CollectionMatcher<? extends String> downstream) {
        return typedWithDownstream(RecordQueryTypeFilterPlan.class,
                Extractor.of(RecordQueryTypeFilterPlan::getRecordTypes, name -> "recordTypes(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnion(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedUnionPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnion(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedUnionPlan.class, downstream);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnionPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryUnorderedUnionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnionPlan(@Nonnull final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return childrenPlans(RecordQueryUnorderedUnionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnionPlan> union(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnionPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnionPlan> union(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnionPlan.class, downstream);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryUnionPlan> unionPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryUnionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnionPlan> unionPlan(@Nonnull final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return childrenPlans(RecordQueryUnionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnionPlan> comparisonKey(@Nonnull BindingMatcher<KeyExpression> comparisonKeyMatcher) {
        return typedWithDownstream(RecordQueryUnionPlan.class,
                Extractor.of(RecordQueryUnionPlan::getComparisonKey, name -> "comparisonKey(" + name + ")"),
                comparisonKeyMatcher);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnionPlan> comparisonKey(@Nonnull KeyExpression probe) {
        return typedWithDownstream(RecordQueryUnionPlan.class,
                Extractor.of(RecordQueryUnionPlan::getComparisonKey, name -> "comparisonKey(" + name + ")"),
                PrimitiveMatchers.equalsObject(probe));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> unorderedPrimaryKeyDistinct(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> unorderedPrimaryKeyDistinct(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> unorderedPrimaryKeyDistinctPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, all(downstream));
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryIntersectionPlan> intersectionPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryIntersectionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    @Nonnull
    public static BindingMatcher<LogicalIntersectionExpression> logicalIntersectionExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalIntersectionExpression.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalIntersectionExpression> logicalIntersectionExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalIntersectionExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryCoveringIndexPlan> coveringIndexPlan() {
        return ofTypeOwning(RecordQueryCoveringIndexPlan.class, CollectionMatcher.empty());
    }

    @Nonnull
    public static BindingMatcher<RecordQueryCoveringIndexPlan> indexPlanOf(@Nonnull BindingMatcher<? extends RecordQueryPlanWithIndex> downstream) {
        return typedWithDownstream(RecordQueryCoveringIndexPlan.class,
                Extractor.of(RecordQueryCoveringIndexPlan::getIndexPlan, name -> "indexPlanOf(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchFromPartialRecord(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFetchFromPartialRecordPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchFromPartialRecord(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFetchFromPartialRecordPlan.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchFromPartialRecordPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFetchFromPartialRecordPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInUnionPlan> inUnion(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInUnionPlan.class, any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInUnionPlan> inUnionPlan(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInUnionPlan.class, all(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInUnionPlan> inUnionComparisonKey(@Nonnull BindingMatcher<KeyExpression> comparisonKeyMatcher) {
        return typedWithDownstream(RecordQueryInUnionPlan.class,
                Extractor.of(RecordQueryInUnionPlan::getComparisonKey, name -> "comparisonKey(" + name + ")"),
                comparisonKeyMatcher);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInUnionPlan> inUnionComparisonKey(@Nonnull KeyExpression probe) {
        return typedWithDownstream(RecordQueryInUnionPlan.class,
                Extractor.of(RecordQueryInUnionPlan::getComparisonKey, name -> "comparisonKey(" + name + ")"),
                PrimitiveMatchers.equalsObject(probe));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryInUnionPlan> inUnionValuesSources(@Nonnull CollectionMatcher<? extends InSource> downstream) {
        return typedWithDownstream(RecordQueryInUnionPlan.class,
                Extractor.of(RecordQueryInUnionPlan::getInSources, name -> "valuesSources(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<InSource> inUnionBindingName(@Nonnull String bindingName) {
        return typedWithDownstream(InSource.class,
                Extractor.of(InSource::getBindingName, name -> "bindingName(" + name + ")"),
                PrimitiveMatchers.equalsObject(bindingName));
    }

    @Nonnull
    public static BindingMatcher<InValuesSource> inUnionInValues(@Nonnull BindingMatcher<? extends Collection<?>> downstream) {
        return typedWithDownstream(InValuesSource.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getValues()), name -> "values(" + name + ")"),
                downstream);
    }
    
    @Nonnull
    public static BindingMatcher<InParameterSource> inUnionInParameter(@Nonnull BindingMatcher<String> downstream) {
        return typedWithDownstream(InParameterSource.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getParameterName()), name -> "parameter(" + name + ")"),
                downstream);
    }

}
