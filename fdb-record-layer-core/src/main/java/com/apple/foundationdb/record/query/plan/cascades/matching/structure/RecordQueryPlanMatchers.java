/*
 * RecordQueryPlanMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAbstractDataModificationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDeletePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInComparandJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisonKeyValues;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableScanPlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableInsertPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.ofTypeOwning;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.SetMatcher.exactlyInAnyOrder;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

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

    public static <R extends RecordQueryPlan> BindingMatcher<R> childrenPlans(final Class<R> bindableClass, final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of((R recordQueryPlan) -> {
                    final List<? extends Quantifier> quantifiers = recordQueryPlan.getQuantifiers();
                    final List<Iterable<RelationalExpression>>
                            rangedOverPlans = quantifiers.stream()
                            .map(quantifier -> quantifier.getRangesOver().getFinalExpressions().stream().collect(ImmutableList.toImmutableList()))
                            .collect(ImmutableList.toImmutableList());
                    return CrossProduct.crossProduct(rangedOverPlans);
                }, name -> "planChildren(" + name + ")"),
                AnyMatcher.anyInIterable(downstream));
    }

    public static BindingMatcher<RecordQueryPlan> descendantPlans(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(plan.collectDescendantPlans()), name -> "descendantPlans(" + name + ")"),
                any(downstream));
    }

    public static BindingMatcher<RecordQueryPlan> descendantPlans(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(plan.collectDescendantPlans()), name -> "descendantPlans(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryPlan> selfOrDescendantPlans(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(Iterables.concat(plan.collectDescendantPlans(), ImmutableList.of(plan))), name -> "selfOrDescendantPlans(" + name + ")"),
                any(downstream));
    }

    public static BindingMatcher<RecordQueryPlan> selfOrDescendantPlans(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(plan -> ImmutableList.copyOf(Iterables.concat(plan.collectDescendantPlans(), ImmutableList.of(plan))), name -> "selfOrDescendantPlans(" + name + ")"),
                downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static ListMatcher<? extends RecordQueryPlan> exactlyPlans(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return exactly(Arrays.asList(downstreams));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static SetMatcher<? extends RecordQueryPlan> exactlyPlansInAnyOrder(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return exactlyInAnyOrder(Arrays.asList(downstreams));
    }

    public static SetMatcher<? extends RecordQueryPlan> exactlyPlansInAnyOrder(final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return exactlyInAnyOrder(downstreams);
    }

    public static BindingMatcher<RecordQueryDefaultOnEmptyPlan> defaultOnEmptyPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryDefaultOnEmptyPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryFilterPlan> filter(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFilterPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryFilterPlan> filterPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFilterPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryFilterPlan> filterPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFilterPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryFilterPlan> queryComponents(CollectionMatcher<? extends QueryComponent> downstream) {
        return typedWithDownstream(RecordQueryFilterPlan.class,
                Extractor.of(RecordQueryFilterPlan::getFilters, name -> "filters(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryIndexPlan> indexPlan() {
        return ofTypeOwning(RecordQueryIndexPlan.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<RecordQueryInJoinPlan> inJoin(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInJoinPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryInJoinPlan> inJoinPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInJoinPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInJoinPlan> inJoinPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInJoinPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameterJoin(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInParameterJoinPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameterJoinPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInParameterJoinPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameterJoinPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInParameterJoinPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryInComparandJoinPlan> inComparandJoin(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInComparandJoinPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryInComparandJoinPlan> inComparandJoinPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInComparandJoinPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInComparandJoinPlan> inComparandJoinPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInComparandJoinPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryInParameterJoinPlan> inParameter(BindingMatcher<String> downstream) {
        return typedWithDownstream(RecordQueryInParameterJoinPlan.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getExternalBinding()), name -> "externalBinding(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesJoin(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInValuesJoinPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesJoinPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInValuesJoinPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesJoinPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInValuesJoinPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryInValuesJoinPlan> inValuesList(BindingMatcher<? extends Collection<?>> downstream) {
        return typedWithDownstream(RecordQueryInValuesJoinPlan.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getInListValues()), name -> "values(" + name + ")"),
                downstream);
    }

    public static <T extends List<?>> BindingMatcher<T> equalsInList(final T object) {
        return PrimitiveMatchers.testObject(object, Comparisons::compareListEquals);
    }

    public static BindingMatcher<RecordQueryPlanWithComparisons> scanComparisons(BindingMatcher<ScanComparisons> scanComparisonsBindingMatcher) {
        return typedWithDownstream(RecordQueryPlanWithComparisons.class,
                Extractor.of(RecordQueryPlanWithComparisons::getScanComparisons, name -> "comparisons(" + name + ")"),
                scanComparisonsBindingMatcher);
    }

    public static BindingMatcher<RecordQueryPlan> isReverse() {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(RecordQueryPlan::isReverse, name -> "isReversed(" + name + ")"),
                PrimitiveMatchers.equalsObject(true));
    }

    public static BindingMatcher<RecordQueryPlan> isNotReverse() {
        return typedWithDownstream(RecordQueryPlan.class,
                Extractor.of(RecordQueryPlan::isReverse, name -> "isNotReversed(" + name + ")"),
                PrimitiveMatchers.equalsObject(false));
    }

    public static BindingMatcher<RecordQueryPlanWithIndex> indexName(String indexName) {
        return typedWithDownstream(RecordQueryPlanWithIndex.class,
                Extractor.of(RecordQueryPlanWithIndex::getIndexName, name -> "indexName(" + name + ")"),
                PrimitiveMatchers.equalsObject(indexName));
    }

    public static BindingMatcher<RecordQueryPlanWithIndex> indexScanType(IndexScanType scanType) {
        return typedWithDownstream(RecordQueryPlanWithIndex.class,
                Extractor.of(RecordQueryPlanWithIndex::getScanType, name -> "indexScanType(" + name + ")"),
                PrimitiveMatchers.equalsObject(scanType));
    }

    public static BindingMatcher<RecordQueryIndexPlan> indexScanParameters(final BindingMatcher<? extends IndexScanParameters> downstream) {
        return typedWithDownstream(RecordQueryIndexPlan.class,
                Extractor.of(RecordQueryIndexPlan::getScanParameters, name -> "indexScanParameters(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<MultidimensionalIndexScanComparisons> multidimensional() {
        return typed(MultidimensionalIndexScanComparisons.class);
    }

    public static BindingMatcher<MultidimensionalIndexScanComparisons> prefix(final BindingMatcher<? extends ScanComparisons> downstream) {
        return typedWithDownstream(MultidimensionalIndexScanComparisons.class,
                Extractor.of(MultidimensionalIndexScanComparisons::getPrefixScanComparisons, name -> "prefix(" + name + ")"),
                downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<MultidimensionalIndexScanComparisons> dimensions(final BindingMatcher<? extends ScanComparisons>... downstreams) {
        return typedWithDownstream(MultidimensionalIndexScanComparisons.class,
                Extractor.of(MultidimensionalIndexScanComparisons::getDimensionsScanComparisons, name -> "dimensions(" + name + ")"),
                exactly(downstreams));
    }

    public static BindingMatcher<MultidimensionalIndexScanComparisons> suffix(final BindingMatcher<? extends ScanComparisons> downstream) {
        return typedWithDownstream(MultidimensionalIndexScanComparisons.class,
                Extractor.of(MultidimensionalIndexScanComparisons::getSuffixScanComparisons, name -> "suffix(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryPredicatesFilterPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryPredicatesFilterPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(final BindingMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                   final BindingMatcher<? extends Quantifier> downstreamQuantifiers) {
        return RelationalExpressionMatchers.ofTypeWithPredicatesAndOwning(RecordQueryPredicatesFilterPlan.class, any(downstreamPredicates), any(downstreamQuantifiers));
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilter(final CollectionMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                   final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return RelationalExpressionMatchers.ofTypeWithPredicatesAndOwning(RecordQueryPredicatesFilterPlan.class, downstreamPredicates, downstreamQuantifiers);
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilterPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryPredicatesFilterPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicatesFilterPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryPredicatesFilterPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicates(CollectionMatcher<? extends QueryPredicate> downstream) {
        return typedWithDownstream(RecordQueryPredicatesFilterPlan.class,
                Extractor.of(RecordQueryPredicatesFilterPlan::getPredicates, name -> "predicates(" + name + ")"),
                downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryPredicatesFilterPlan> predicates(BindingMatcher<? extends QueryPredicate>... downstreams) {
        return typedWithDownstream(RecordQueryPredicatesFilterPlan.class,
                Extractor.of(RecordQueryPredicatesFilterPlan::getPredicates, name -> "predicates(" + name + ")"),
                exactlyInAnyOrder(downstreams));
    }

    public static BindingMatcher<RecordQueryScanPlan> scanPlan() {
        return ofTypeOwning(RecordQueryScanPlan.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<TempTableScanPlan> tempTableScanPlan() {
        return ofTypeOwning(TempTableScanPlan.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilter(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryTypeFilterPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilter(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryTypeFilterPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilterPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryTypeFilterPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryTypeFilterPlan> typeFilterPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryTypeFilterPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryTypeFilterPlan> recordTypes(CollectionMatcher<? extends String> downstream) {
        return typedWithDownstream(RecordQueryTypeFilterPlan.class,
                Extractor.of(RecordQueryTypeFilterPlan::getRecordTypes, name -> "recordTypes(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnion(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedUnionPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnion(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedUnionPlan.class, downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnionPlan(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryUnorderedUnionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    public static BindingMatcher<RecordQueryUnorderedUnionPlan> unorderedUnionPlan(final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return childrenPlans(RecordQueryUnorderedUnionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    public static BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> unionOnExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnionOnKeyExpressionPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> unionOnExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnionOnKeyExpressionPlan.class, downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> unionOnExpressionPlan(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryUnionOnKeyExpressionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    public static BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> unionOnExpressionPlan(final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return childrenPlans(RecordQueryUnionOnKeyExpressionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    public static BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> comparisonKey(BindingMatcher<KeyExpression> comparisonKeyMatcher) {
        return typedWithDownstream(RecordQueryUnionOnKeyExpressionPlan.class,
                Extractor.of(RecordQueryUnionOnKeyExpressionPlan::getComparisonKeyExpression, name -> "comparisonKey(" + name + ")"),
                comparisonKeyMatcher);
    }

    public static BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> comparisonKey(KeyExpression probe) {
        return typedWithDownstream(RecordQueryUnionOnKeyExpressionPlan.class,
                Extractor.of(RecordQueryUnionOnKeyExpressionPlan::getComparisonKeyExpression, name -> "comparisonKey(" + name + ")"),
                PrimitiveMatchers.equalsObject(probe));
    }

    public static BindingMatcher<RecordQueryUnionOnValuesPlan> unionOnValue(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnionOnValuesPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryUnionOnValuesPlan> unionOnValue(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnionOnValuesPlan.class, downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryUnionOnValuesPlan> unionOnValuesPlan(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryUnionOnValuesPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    public static BindingMatcher<RecordQueryUnionOnValuesPlan> unionOnValuesPlan(final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return childrenPlans(RecordQueryUnionOnValuesPlan.class, exactlyPlansInAnyOrder(downstreams));
    }
    
    @SuppressWarnings("unchecked")
    public static BindingMatcher<RecordQueryPlanWithComparisonKeyValues> comparisonKeyValues(CollectionMatcher<? extends Value> comparisonKeyMatcher) {
        return typedWithDownstream(RecordQueryPlanWithComparisonKeyValues.class,
                Extractor.of(RecordQueryPlanWithComparisonKeyValues::getComparisonKeyValues, name -> "comparisonKeyValues(" + name + ")"),
                comparisonKeyMatcher);
    }

    public static BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> unorderedPrimaryKeyDistinct(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> unorderedPrimaryKeyDistinct(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> unorderedPrimaryKeyDistinctPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, all(downstream));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryIntersectionOnKeyExpressionPlan> intersectionOnExpressionPlan(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryIntersectionOnKeyExpressionPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordQueryIntersectionOnValuesPlan> intersectionOnValuesPlan(final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return childrenPlans(RecordQueryIntersectionOnValuesPlan.class, exactlyPlansInAnyOrder(downstreams));
    }

    public static BindingMatcher<LogicalIntersectionExpression> logicalIntersectionExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalIntersectionExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalIntersectionExpression> logicalIntersectionExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalIntersectionExpression.class, downstream);
    }

    public static BindingMatcher<RecordQueryCoveringIndexPlan> coveringIndexPlan() {
        return ofTypeOwning(RecordQueryCoveringIndexPlan.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<RecordQueryCoveringIndexPlan> indexPlanOf(BindingMatcher<? extends RecordQueryPlanWithIndex> downstream) {
        return typedWithDownstream(RecordQueryCoveringIndexPlan.class,
                Extractor.of(RecordQueryCoveringIndexPlan::getIndexPlan, name -> "indexPlanOf(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchFromPartialRecord(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFetchFromPartialRecordPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchFromPartialRecord(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFetchFromPartialRecordPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchFromPartialRecordPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFetchFromPartialRecordPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInUnionOnKeyExpressionPlan> inUnionOnExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInUnionOnKeyExpressionPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryInUnionOnKeyExpressionPlan> inUnionOnExpressionPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInUnionOnKeyExpressionPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInUnionOnKeyExpressionPlan> inUnionComparisonKey(BindingMatcher<KeyExpression> comparisonKeyMatcher) {
        return typedWithDownstream(RecordQueryInUnionOnKeyExpressionPlan.class,
                Extractor.of(RecordQueryInUnionOnKeyExpressionPlan::getComparisonKeyExpression, name -> "comparisonKeyExpression(" + name + ")"),
                comparisonKeyMatcher);
    }

    public static BindingMatcher<RecordQueryInUnionOnKeyExpressionPlan> inUnionComparisonKey(KeyExpression probe) {
        return typedWithDownstream(RecordQueryInUnionOnKeyExpressionPlan.class,
                Extractor.of(RecordQueryInUnionOnKeyExpressionPlan::getComparisonKeyExpression, name -> "comparisonKeyExpression(" + name + ")"),
                PrimitiveMatchers.equalsObject(probe));
    }

    public static BindingMatcher<RecordQueryInUnionOnValuesPlan> inUnionOnValues(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryInUnionOnValuesPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryInUnionOnValuesPlan> inUnionOnValuesPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInUnionOnValuesPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInUnionOnValuesPlan> inUnionComparisonValues(CollectionMatcher<? extends Value> comparisonValuesMatcher) {
        return typedWithDownstream(RecordQueryInUnionOnValuesPlan.class,
                Extractor.of(RecordQueryInUnionOnValuesPlan::getComparisonKeyValues, name -> "comparisonValues(" + name + ")"),
                comparisonValuesMatcher);
    }

    public static BindingMatcher<RecordQueryInUnionPlan> inUnionValuesSources(CollectionMatcher<? extends InSource> downstream) {
        return typedWithDownstream(RecordQueryInUnionPlan.class,
                Extractor.of(RecordQueryInUnionPlan::getInSources, name -> "valuesSources(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<InSource> inUnionBindingName(String bindingName) {
        return typedWithDownstream(InSource.class,
                Extractor.of(InSource::getBindingName, name -> "bindingName(" + name + ")"),
                PrimitiveMatchers.equalsObject(bindingName));
    }

    public static BindingMatcher<InValuesSource> inUnionInValues(BindingMatcher<? extends Collection<?>> downstream) {
        return typedWithDownstream(InValuesSource.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getValues()), name -> "values(" + name + ")"),
                downstream);
    }
    
    public static BindingMatcher<InParameterSource> inUnionInParameter(BindingMatcher<String> downstream) {
        return typedWithDownstream(InParameterSource.class,
                Extractor.of(plan -> Objects.requireNonNull(plan.getParameterName()), name -> "parameter(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryMapPlan> map(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryMapPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryMapPlan> map(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryMapPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryMapPlan> mapPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryMapPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryMapPlan> mapPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryMapPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryMapPlan> mapResult(BindingMatcher<? extends Value> downstream) {
        return typedWithDownstream(RecordQueryMapPlan.class,
                Extractor.of(RecordQueryMapPlan::getResultValue, name -> "result(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryFlatMapPlan> flatMap(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFlatMapPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryFlatMapPlan> flatMapPlan(final BindingMatcher<? extends RecordQueryPlan> downstream1,
                                                                     final BindingMatcher<? extends RecordQueryPlan> downstream2) {
        return childrenPlans(RecordQueryFlatMapPlan.class, exactly(downstream1, downstream2));
    }

    public static BindingMatcher<RecordQueryMapPlan> flatMapPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryMapPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryFlatMapPlan> flatMapResult(BindingMatcher<? extends Value> downstream) {
        return typedWithDownstream(RecordQueryFlatMapPlan.class,
                Extractor.of(RecordQueryFlatMapPlan::getResultValue, name -> "result(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryFirstOrDefaultPlan> firstOrDefault(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecordQueryFirstOrDefaultPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryFirstOrDefaultPlan> firstOrDefaultPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFirstOrDefaultPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryFirstOrDefaultPlan> firstOrDefaultPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryFirstOrDefaultPlan.class, downstream);
    }

    public static BindingMatcher<RecordQueryFirstOrDefaultPlan> onEmptyResult(BindingMatcher<? extends Value> downstream) {
        return typedWithDownstream(RecordQueryFirstOrDefaultPlan.class,
                Extractor.of(RecordQueryFirstOrDefaultPlan::getOnEmptyResultValue, name -> "onEmptyResult(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryStreamingAggregationPlan> streamingAggregationPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryStreamingAggregationPlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryStreamingAggregationPlan> aggregations(BindingMatcher<? extends Value> downstream) {
        return typedWithDownstream(RecordQueryStreamingAggregationPlan.class,
                Extractor.of(RecordQueryStreamingAggregationPlan::getAggregateValue, name -> "aggregation(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryStreamingAggregationPlan> groupings(BindingMatcher<? extends Value> downstream) {
        return typedWithDownstream(RecordQueryStreamingAggregationPlan.class,
                Extractor.of(RecordQueryStreamingAggregationPlan::getGroupingValue, name -> "grouping(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryAggregateIndexPlan> aggregateIndexPlan() {
        return ofTypeOwning(RecordQueryAggregateIndexPlan.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<RecordQueryAggregateIndexPlan> aggregateIndexPlanOf(BindingMatcher<? extends RecordQueryPlanWithIndex> downstream) {
        return typedWithDownstream(RecordQueryAggregateIndexPlan.class,
                Extractor.of(RecordQueryAggregateIndexPlan::getIndexPlan, name -> "indexPlanOf(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<ComposedBitmapIndexQueryPlan> composedBitmapPlan(final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(ComposedBitmapIndexQueryPlan.class, Extractor.of(ComposedBitmapIndexQueryPlan::getIndexPlans, name -> "indexPlans(" + name + ")"), downstream);
    }

    public static BindingMatcher<ComposedBitmapIndexQueryPlan> composer(final BindingMatcher<ComposedBitmapIndexQueryPlan.ComposerBase> downstream) {
        return typedWithDownstream(ComposedBitmapIndexQueryPlan.class, Extractor.of(ComposedBitmapIndexQueryPlan::getComposer, name -> "composer(" + name + ")"), downstream);
    }

    public static BindingMatcher<ComposedBitmapIndexQueryPlan.ComposerBase> composition(final String compositionString) {
        return typedWithDownstream(ComposedBitmapIndexQueryPlan.ComposerBase.class, Extractor.of(ComposedBitmapIndexQueryPlan.ComposerBase::toString, name -> "composition(" + name + ")"), PrimitiveMatchers.equalsObject(compositionString));
    }

    public static BindingMatcher<RecordQueryExplodePlan> explodePlan() {
        return ofTypeOwning(RecordQueryExplodePlan.class, CollectionMatcher.empty());
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<RecordQueryExplodePlan> collectionValue(BindingMatcher<? extends Value> downstream) {
        return typedWithDownstream(RecordQueryExplodePlan.class,
                Extractor.of(RecordQueryExplodePlan::getCollectionValue, name -> "collectionValue(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecordQueryDeletePlan> deletePlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryDeletePlan.class, all(downstream));
    }

    public static BindingMatcher<RecordQueryInsertPlan> insertPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryInsertPlan.class, exactlyPlans(downstream));
    }

    public static BindingMatcher<TempTableInsertPlan> tempTableInsertPlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(TempTableInsertPlan.class, exactlyPlans(downstream));
    }

    public static BindingMatcher<TempTableInsertPlan> tempTableInsertPlanOverQuantifier(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(TempTableInsertPlan.class, any(downstream));
    }

    public static BindingMatcher<RecordQueryUpdatePlan> updatePlan(final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return childrenPlans(RecordQueryUpdatePlan.class, all(downstream));
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<RecordQueryAbstractDataModificationPlan> target(BindingMatcher<? extends String> downstream) {
        return typedWithDownstream(RecordQueryAbstractDataModificationPlan.class,
                Extractor.of(RecordQueryAbstractDataModificationPlan::getTargetRecordType, name -> "target(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<RecursiveUnionExpression> dfsTraversalAllowed() {
        return typedWithDownstream(RecursiveUnionExpression.class,
                Extractor.of(RecursiveUnionExpression::dfsTraversalAllowed, name -> "dfsTraversal(" + name + ")"),
                PrimitiveMatchers.equalsObject(true));
    }

    public static BindingMatcher<RecursiveUnionExpression> levelTraversalIsAllowed() {
        return typedWithDownstream(RecursiveUnionExpression.class,
                Extractor.of(RecursiveUnionExpression::levelTraversalAllowed, name -> "levelTraversal(" + name + ")"),
                PrimitiveMatchers.equalsObject(true));
    }

    public static BindingMatcher<SelectExpression> hasNoPredicates() {
        return typedWithDownstream(SelectExpression.class,
                Extractor.of(SelectExpression::hasPredicates, name -> "levelTraversal(" + name + ")"),
                PrimitiveMatchers.equalsObject(false));
    }
}
