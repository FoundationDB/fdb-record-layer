/*
 * QueryRecordFunctionWithComparison.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionExpansionVisitor;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionExpansionVisitor.VisitorState;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.RankValue;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} whose value from the record,
 * to be compared with the comparison's value, is the result of applying a {@link RecordFunction} to the record.
 */
@API(API.Status.MAINTAINED)
public class QueryRecordFunctionWithComparison implements ComponentWithComparison {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Query-Record-Function-With-Comparison");

    @Nonnull
    private final RecordFunction<?> function;
    @Nonnull
    private final Comparisons.Comparison comparison;

    public QueryRecordFunctionWithComparison(@Nonnull RecordFunction<?> function, @Nonnull Comparisons.Comparison comparison) {
        this.function = function;
        this.comparison = comparison;
    }

    @Nonnull
    public RecordFunction<?> getFunction() {
        return function;
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new QueryRecordFunctionWithComparison(function, comparison);
    }

    @Override
    public String getName() {
        return function.toString();
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        return store.getContext().join(evalMessageAsync(store, context, record, message));
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Boolean> evalMessageAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                           @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (record == null) {
            return CompletableFuture.completedFuture(getComparison().eval(store, context, null));
        }
        return store.evaluateRecordFunction(context, function, record).thenApply(value -> getComparison().eval(store, context, value));
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        function.validate(descriptor);
    }

    @Nonnull
    @Override
    public GraphExpansion expand(@Nonnull final CorrelationIdentifier baseAlias,
                                 @Nonnull Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nonnull final List<String> fieldNamePrefix) {

        // TODO for now we only do this for rank but we can do this for more than that
        if (function instanceof IndexRecordFunction && function.getName().equals(FunctionNames.RANK)) {
            final var groupingKeyExpression = ((IndexRecordFunction<?>)function).getOperand();
            final var wholeKeyExpression = groupingKeyExpression.getWholeKey();
            final var expansionVisitor = new KeyExpressionExpansionVisitor();
            final var innerBaseQuantifier = baseQuantifierSupplier.get();
            final var partitioningAndArgumentExpansion =
                    wholeKeyExpression.expand(
                            expansionVisitor.push(VisitorState.forQueries(Lists.newArrayList(),
                                    innerBaseQuantifier.getAlias(),
                                    fieldNamePrefix)));
            final var sealedPartitioningAndArgumentExpansion = partitioningAndArgumentExpansion.seal();

            // construct a select expression that uses a windowed value to express the rank
            final var partitioningSize = groupingKeyExpression.getGroupingCount();
            final var partitioningExpressions = sealedPartitioningAndArgumentExpansion.getResultValues().subList(0, partitioningSize);
            final var argumentExpressions = sealedPartitioningAndArgumentExpansion.getResultValues().subList(partitioningSize, groupingKeyExpression.getColumnSize());
            final var rankValue = new RankValue(partitioningExpressions, argumentExpressions);
            final var rankExpansion =
                    GraphExpansion.ofOthers(partitioningAndArgumentExpansion,
                            GraphExpansion.ofResultValueAndQuantifier(rankValue, innerBaseQuantifier));
            final var rankSelectExpression = rankExpansion.buildSelect();
            final var rankQuantifier = Quantifier.forEach(GroupExpressionRef.of(rankSelectExpression));

            //
            // Construct another select expression that applies the predicate on the rank value as well as adds a join
            // predicate between the outer and the inner base as we model this index access as a semi join
            //

            // predicate on rank
            final var rankColumnValue = QuantifiedColumnValue.of(rankQuantifier.getAlias(), rankSelectExpression.getResultValues().size() - 1);
            final var rankComparisonExpansion =
                    GraphExpansion.ofPredicateAndQuantifier(new ValuePredicate(rankColumnValue, comparison),
                            rankQuantifier);

            // join predicate
            final var selfJoinPredicate =
                    QuantifiedObjectValue.of(rankQuantifier.getAlias())
                            .withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS,
                                    Bindings.Internal.CORRELATION.bindingName(baseAlias.toString()), Bindings.Internal.CORRELATION));
            final var selfJoinPredicateExpansion = GraphExpansion.ofPredicate(selfJoinPredicate);

            final var rankAndJoiningPredicateSelectExpression =
                    GraphExpansion.ofOthers(rankComparisonExpansion, selfJoinPredicateExpansion).buildSelect();
            final var rankComparisonQuantifier =
                    Quantifier.existential(GroupExpressionRef.of(rankAndJoiningPredicateSelectExpression));

            // create a query component that creates a path to this prefix and then applies this to it
            // this is needed for reapplication of the component if the sub query cannot be matched or only matched with
            // compensation
            QueryComponent withPrefix = this;
            for (int i = fieldNamePrefix.size() - 1; i >= 0;  i--) {
                final String fieldName = fieldNamePrefix.get(i);
                withPrefix = Query.field(fieldName).matches(withPrefix);
            }

            return GraphExpansion.ofPredicateAndQuantifier(new ExistsPredicate(rankComparisonQuantifier.getAlias(), withPrefix), rankComparisonQuantifier);
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return function + " " + getComparison();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryRecordFunctionWithComparison that = (QueryRecordFunctionWithComparison) o;
        return Objects.equals(function, that.function) &&
               Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, getComparison());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return function.planHash(hashKind) + getComparison().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH, function, getComparison());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, function, getComparison());
    }
}
