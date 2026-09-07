/*
 * RelationalExpressionMatchers.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.DeleteExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUniqueExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for descendants of {@link RelationalExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class RelationalExpressionMatchers {
    private RelationalExpressionMatchers() {
        // do not instantiate
    }

    public static BindingMatcher<RelationalExpression> anyExpression() {
        return typed(RelationalExpression.class);
    }

    public static <R extends RelationalExpression> TypedMatcher<R> ofType(final Class<R> bindableClass) {
        return typed(bindableClass);
    }

    public static <R extends RelationalExpression> BindingMatcher<R> ofType(final Class<R> bindableClass,
                                                                            final BindingMatcher<R> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.identity(),
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicates(final Class<R> bindableClass,
                                                                                                                                                    final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(RelationalExpressionWithPredicates::getPredicates, name -> "predicates(" + name + ")"),
                downstream);
    }

    public static <R extends RelationalExpression, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeOwning(final Class<R> bindableClass,
                                                                                                                              final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(RelationalExpression::getQuantifiers, name -> "quantifiers(" + name + ")"),
                downstream);
    }

    public static <R extends RelationalExpression> BindingMatcher<R> canBeImplemented() {
        return PrimitiveMatchers.satisfies(relationalExpression ->
                relationalExpression.getQuantifiers()
                        .stream()
                        .noneMatch(quantifier -> quantifier.getRangesOver().getFinalExpressions().isEmpty()));
    }

    public static <R extends RelationalExpressionWithPredicates, C1 extends Collection<? extends QueryPredicate>, C2 extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicatesAndOwning(final Class<R> bindableClass,
                                                                                                                                                                                                               final BindingMatcher<C1> downstreamPredicates,
                                                                                                                                                                                                               final BindingMatcher<C2> downstreamQuantifiers) {
        return typedWithDownstream(bindableClass,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(RelationalExpressionWithPredicates.class,
                        ImmutableList.of(
                                typedWithDownstream(bindableClass,
                                        Extractor.of(RelationalExpressionWithPredicates::getPredicates, name -> "predicates(" + name + ")"),
                                        downstreamPredicates),
                                typedWithDownstream(bindableClass,
                                        Extractor.of(RelationalExpression::getQuantifiers, name -> "quantifiers(" + name + ")"),
                                        downstreamQuantifiers))));
    }

    public static <Q extends Collection<? extends Quantifier>> BindingMatcher<RelationalExpression>
            anyExploratoryExpression(final BindingMatcher<Q> downstreamQuantifiers) {
        return typedWithDownstream(RelationalExpression.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(RelationalExpression.class,
                        ImmutableList.of(
                                PrimitiveMatchers.<RelationalExpression, Reference>satisfiesWithOuterBinding(ReferenceMatchers.getCurrentReferenceMatcher(),
                                        (expression, currentReference) -> !currentReference.isFinal(expression)),
                                typedWithDownstream(RelationalExpression.class,
                                        Extractor.of(RelationalExpression::getQuantifiers, name -> "quantifiers(" + name + ")"),
                                        downstreamQuantifiers))));
    }

    public static <E extends RelationalExpression> BindingMatcher<E> isExploratoryExpression() {
        return PrimitiveMatchers.satisfiesWithOuterBinding(ReferenceMatchers.getCurrentReferenceMatcher(),
                (expression, currentReference) -> !currentReference.isFinal(expression));
    }

    public static <E extends RelationalExpression> BindingMatcher<E> isFinalExpression() {
        return PrimitiveMatchers.satisfiesWithOuterBinding(ReferenceMatchers.getCurrentReferenceMatcher(),
                (expression, currentReference) -> currentReference.isFinal(expression));
    }

    public static BindingMatcher<FullUnorderedScanExpression> fullUnorderedScanExpression() {
        return ofTypeOwning(FullUnorderedScanExpression.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<LogicalDistinctExpression> logicalDistinctExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalDistinctExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalDistinctExpression> logicalDistinctExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalDistinctExpression.class, downstream);
    }

    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalFilterExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalFilterExpression.class, downstream);
    }

    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(final BindingMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                  final BindingMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(LogicalFilterExpression.class, any(downstreamPredicates), any(downstreamQuantifiers));
    }

    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(final CollectionMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                  final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(LogicalFilterExpression.class, downstreamPredicates, downstreamQuantifiers);
    }

    public static BindingMatcher<LogicalProjectionExpression> logicalProjectionExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalProjectionExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalProjectionExpression> logicalProjectionExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalProjectionExpression.class, downstream);
    }

    public static BindingMatcher<LogicalSortExpression> logicalSortExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalSortExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalSortExpression> logicalSortExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalSortExpression.class, downstream);
    }

    public static BindingMatcher<LogicalTypeFilterExpression> logicalTypeFilterExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalTypeFilterExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalTypeFilterExpression> logicalTypeFilterExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalTypeFilterExpression.class, downstream);
    }

    public static BindingMatcher<LogicalUnionExpression> logicalUnionExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalUnionExpression.class, any(downstream));
    }

    public static BindingMatcher<LogicalUnionExpression> logicalUnionExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalUnionExpression.class, downstream);
    }

    public static BindingMatcher<LogicalUniqueExpression> logicalUniqueExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalUniqueExpression.class, downstream);
    }

    public static BindingMatcher<SelectExpression> selectExpression() {
        return ofType(SelectExpression.class);
    }

    public static BindingMatcher<RelationalExpressionWithPredicates> withPredicatesExpression() {
        return ofType(RelationalExpressionWithPredicates.class);
    }

    public static BindingMatcher<SelectExpression> selectExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(SelectExpression.class, any(downstream));
    }

    public static BindingMatcher<SelectExpression> selectExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(SelectExpression.class, downstream);
    }

    public static BindingMatcher<SelectExpression> selectExpression(final BindingMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                    final BindingMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(SelectExpression.class, any(downstreamPredicates), any(downstreamQuantifiers));
    }

    public static BindingMatcher<SelectExpression> selectExpression(final CollectionMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                    final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(SelectExpression.class, downstreamPredicates, downstreamQuantifiers);
    }

    public static BindingMatcher<OuterJoinExpression> outerJoinExpression() {
        return ofType(OuterJoinExpression.class);
    }

    public static BindingMatcher<ExplodeExpression> explodeExpression() {
        return ofTypeOwning(ExplodeExpression.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<TableFunctionExpression> tableFunctionExpression() {
        return ofTypeOwning(TableFunctionExpression.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<GroupByExpression> groupByExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(GroupByExpression.class, only(downstream));
    }

    public static BindingMatcher<GroupByExpression> groupByExpression(final BindingMatcher<? extends RecordConstructorValue> downstreamAggregation,
                                                                      final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return typedWithDownstream(GroupByExpression.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(GroupByExpression.class,
                        ImmutableList.of(
                                typedWithDownstream(GroupByExpression.class,
                                        Extractor.of(GroupByExpression::getAggregateValue, name -> "aggregation(" + name + ")"),
                                        downstreamAggregation),
                                typedWithDownstream(GroupByExpression.class,
                                        Extractor.of(RelationalExpression::getQuantifiers, name -> "quantifiers(" + name + ")"),
                                        downstreamQuantifiers))));
    }

    public static BindingMatcher<DeleteExpression> deleteExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(DeleteExpression.class, only(downstream));
    }

    public static BindingMatcher<InsertExpression> insertExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(InsertExpression.class, only(downstream));
    }

    public static BindingMatcher<TempTableInsertExpression> tempTableInsertExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(TempTableInsertExpression.class, only(downstream));
    }

    public static BindingMatcher<UpdateExpression> updateExpression(final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(UpdateExpression.class, only(downstream));
    }

    public static BindingMatcher<TempTableScanExpression> tempTableScanExpression() {
        return ofTypeOwning(TempTableScanExpression.class, CollectionMatcher.empty());
    }

    public static BindingMatcher<RecursiveUnionExpression> recursiveUnionExpression(final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(RecursiveUnionExpression.class, downstream);
    }

    public static BindingMatcher<RecursiveUnionExpression> recursiveUnionExpression(final BindingMatcher<? extends Quantifier> initialDownstream,
                                                                                    final BindingMatcher<? extends Quantifier> recursiveDownstream) {
        return typedWithDownstream(RecursiveUnionExpression.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(RecursiveUnionExpression.class,
                        ImmutableList.of(
                                typedWithDownstream(RecursiveUnionExpression.class,
                                        Extractor.of(RecursiveUnionExpression::getInitialStateQuantifier, name -> "initial(" + name + ")"),
                                        initialDownstream),
                                typedWithDownstream(RecursiveUnionExpression.class,
                                        Extractor.of(RecursiveUnionExpression::getRecursiveStateQuantifier, name -> "recursive(" + name + ")"),
                                        recursiveDownstream))));
    }
}
