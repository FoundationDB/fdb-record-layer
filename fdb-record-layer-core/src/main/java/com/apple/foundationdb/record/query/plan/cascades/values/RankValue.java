/*
 * RankValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRankValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A windowed value that computes the RANK of a list of expressions which can optionally be partitioned by expressions
 * defining a window.
 */
@API(API.Status.EXPERIMENTAL)
public class RankValue extends WindowedValue implements Value.IndexOnlyValue {
    private static final String NAME = "RANK";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public RankValue(@Nonnull final PlanSerializationContext serializationContext,
                     @Nonnull final PRankValue rankValueProto) {
        super(serializationContext, Objects.requireNonNull(rankValueProto.getSuper()));
    }

    public RankValue(@Nonnull Iterable<? extends Value> partitioningValues,
                     @Nonnull Iterable<? extends Value> argumentValues) {
        super(partitioningValues, argumentValues);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public Optional<QueryPredicate> absorbUpperPredicate(@Nonnull final Comparisons.Type comparisonType, @Nonnull final Value comparand) {
        // this enables the rank predicate to consume an upper predicate to produce a distance rank comparison with the
        // correct comparison ranges.
        // Currently, only the following tree structure below is matched and transformed:
        //
        //                   RelOpComparison (<=) (supported comparison operators: <=, <, and =).
        //                   /                \
        //               /                       \           =>      ValuePredicate(EDR(Prtn, FV'), DRVC(<=, Cov'))
        //       Rank(Prtn, [ED(Fv', CoV')])     42'
        //
        // ED=EuclideanDistance ArithmeticValue
        // EDR=EuclideanDistanceRank
        // DRVC=DistanceRangeValueComparison
        if (!(comparand instanceof ConstantObjectValue)) {
            return Optional.empty();
        }
        if (getArgumentValues().size() > 1) {
            return Optional.empty();
        }
        final var argument = getArgumentValues().get(0);
        if (!(argument instanceof ArithmeticValue)) {
            return Optional.empty();
        }
        final var arithmeticValue = (ArithmeticValue)argument;
        if (arithmeticValue.getLogicalOperator() != ArithmeticValue.LogicalOperator.EUCLIDEAN_DISTANCE) {
            return Optional.empty();
        }
        final var euclideanDistanceArgs = ImmutableList.copyOf(arithmeticValue.getChildren());
        final var firstArg = euclideanDistanceArgs.get(0);
        final var secondArg = euclideanDistanceArgs.get(1);
        if (!(firstArg instanceof FieldValue && (secondArg instanceof ConstantObjectValue || secondArg instanceof LiteralValue<?>))) {
            return Optional.empty();
        }

        final Comparisons.Type distanceRankComparisonType;
        switch (comparisonType) {
            case EQUALS:
                distanceRankComparisonType = Comparisons.Type.DISTANCE_RANK_EQUALS;
                break;
            case LESS_THAN:
                distanceRankComparisonType = Comparisons.Type.DISTANCE_RANK_LESS_THAN;
                break;
            case LESS_THAN_OR_EQUALS:
                distanceRankComparisonType = Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL;
                break;
            default:
                return Optional.empty();
        }
        final var value = new EuclideanDistanceRankValue(getPartitioningValues(), ImmutableList.of(firstArg));
        final var comparison = new Comparisons.DistanceRankValueComparison(distanceRankComparisonType, secondArg, comparand);
        return Optional.of(new ValuePredicate(value, comparison));
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return basePlanHash(mode, BASE_HASH);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    public RankValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new RankValue(childrenPair.getKey(), childrenPair.getValue());
    }

    @Nonnull
    @Override
    public PRankValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRankValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRankValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RankValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PRankValue rankValueProto) {
        return new RankValue(serializationContext, rankValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRankValue, RankValue> {
        @Nonnull
        @Override
        public Class<PRankValue> getProtoMessageClass() {
            return PRankValue.class;
        }

        @Nonnull
        @Override
        public RankValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PRankValue rankValueProto) {
            return RankValue.fromProto(serializationContext, rankValueProto);
        }
    }

    /**
     * The {@code rank} window function.
     */
    @AutoService(BuiltInFunction.class)
    public static class RankFn extends BuiltInFunction<Value> {

        public RankFn() {
            super("rank", ImmutableList.of(Type.any(), Type.any()), RankFn::encapsulateInternal);
        }

        @Nonnull
        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static RankValue encapsulateInternal(@Nonnull BuiltInFunction<Value> builtInFunction,
                                                     @Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 2); // ordering expressions must be present, ok for now.
            final var partitioningValuesList = (AbstractArrayConstructorValue)arguments.get(0);
            final var argumentValuesList = (AbstractArrayConstructorValue)arguments.get(1);
            return new RankValue(partitioningValuesList.getChildren(), argumentValuesList.getChildren());
        }
    }
}
