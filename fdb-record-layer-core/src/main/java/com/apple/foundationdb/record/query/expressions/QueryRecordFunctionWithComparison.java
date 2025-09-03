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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.StoreRecordFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionExpansionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionExpansionVisitor.VisitorState;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedRecordValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankValue;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;
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
@API(API.Status.UNSTABLE)
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
        return function.getName();
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> rec, @Nullable Message message) {
        return store.getContext().join(evalMessageAsync(store, context, rec, message));
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Boolean> evalMessageAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                           @Nullable FDBRecord<M> rec, @Nullable Message message) {
        if (rec == null) {
            return CompletableFuture.completedFuture(getComparison().eval(store, context, null));
        }
        return store.evaluateRecordFunction(context, function, rec).thenApply(value -> getComparison().eval(store, context, value));
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        function.validate(descriptor);
    }

    @Nonnull
    @Override
    public GraphExpansion expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                 @Nonnull final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 @Nonnull final List<String> fieldNamePrefix) {
        // TODO for now we only do this for rank but we can do this for more than that
        if (function instanceof IndexRecordFunction && FunctionNames.RANK.equals(function.getName())) {
            final var groupingKeyExpression = ((IndexRecordFunction<?>)function).getOperand();
            final var wholeKeyExpression = groupingKeyExpression.getWholeKey();
            final var expansionVisitor = new KeyExpressionExpansionVisitor();
            final var innerBaseQuantifier = outerQuantifierSupplier.get();
            final var partitioningAndArgumentExpansion =
                    wholeKeyExpression.expand(
                            expansionVisitor.push(VisitorState.forQueries(Lists.newArrayList(),
                                    innerBaseQuantifier,
                                    fieldNamePrefix)));
            final var sealedPartitioningAndArgumentExpansion = partitioningAndArgumentExpansion.seal();

            // construct a select expression that uses a windowed value to express the rank
            final var partitioningSize = groupingKeyExpression.getGroupingCount();
            final var partitioningExpressions = sealedPartitioningAndArgumentExpansion.getResultValues().subList(0, partitioningSize);
            final var argumentExpressions = sealedPartitioningAndArgumentExpansion.getResultValues().subList(partitioningSize, groupingKeyExpression.getColumnSize());
            final var rankValue = new RankValue(partitioningExpressions, argumentExpressions);
            final var rankPredicate = new ValuePredicate(rankValue, comparison);
            final var selfJoinPredicate =
                    innerBaseQuantifier.getFlowedObjectValue()
                            .withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                    QuantifiedObjectValue.of(baseQuantifier.getAlias(), baseQuantifier.getFlowedObjectType())));

            final var rankExpansionBuilder =
                    partitioningAndArgumentExpansion
                            .toBuilder()
                            .removeAllResultColumns()
                            .addPredicate(rankPredicate)
                            .addPredicate(selfJoinPredicate);

            rankExpansionBuilder.pullUpQuantifier(innerBaseQuantifier);
            partitioningAndArgumentExpansion.getQuantifiers()
                    .forEach(quantifier -> rankExpansionBuilder.addAllResultColumns(quantifier.getFlowedColumns()));

            final var rankSelectExpression = rankExpansionBuilder.build().buildSelect();
            final var rankComparisonQuantifier =
                    Quantifier.existential(Reference.initialOf(rankSelectExpression));
            return GraphExpansion.ofExists(rankComparisonQuantifier);
        } else if (function instanceof StoreRecordFunction<?> && FunctionNames.VERSION.equals(function.getName())) {
            final VersionValue versionValue = new VersionValue(QuantifiedRecordValue.of(baseQuantifier));
            return GraphExpansion.builder()
                    .addPredicate(new ValuePredicate(versionValue, comparison))
                    .build();
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return function.planHash(mode) + getComparison().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH, function, getComparison());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

}
