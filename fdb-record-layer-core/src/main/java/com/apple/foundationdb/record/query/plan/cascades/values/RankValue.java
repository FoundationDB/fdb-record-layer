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
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInWindowFunction;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

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

    public RankValue(@Nonnull final Iterable<? extends Value> partitioningValues,
                     @Nonnull final Iterable<OrderingPart.RequestedOrderingPart> orderingParts,
                     @Nonnull final FrameSpecification frameSpecification) {
        super(partitioningValues, ImmutableList.of(), orderingParts, frameSpecification);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
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

    @AutoService(BuiltInFunction.class)
    public static final class RankValueFn extends BuiltInWindowFunction<RankValue> {

        RankValueFn() {
            super("rank", ImmutableList.of(), RankValueFn::encapsulateInternal);
        }

        @Nonnull
        @Override
        public BuiltInFunction<RankValue> createWindowedInstance(@Nullable final FrameSpecification frameSpecification,
                                                                 @Nullable final ImmutableList<OrderingPart.RequestedOrderingPart> providedOrderingParts) {
            return new BuiltInFunction<>("rank", ImmutableList.of(), (ignored, args) -> RankValueFn.encapsulateInternal(args, frameSpecification, providedOrderingParts));
        }

        @Nonnull
        private static RankValue encapsulateInternal(@Nonnull BuiltInFunction<? extends Value> builtInFunction,
                                                     List<? extends Typed> arguments) {
            // calling convention:
            SemanticException.check(arguments.size() >= 2,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            SemanticException.check(arguments.get(0) instanceof AbstractArrayConstructorValue,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            SemanticException.check(arguments.get(1) instanceof AbstractArrayConstructorValue,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);

            final var partitioningValuesList = (AbstractArrayConstructorValue)arguments.get(0);
            final var argumentValuesList = (AbstractArrayConstructorValue)arguments.get(1);
            return null;
        }

        @Nonnull
        private static RankValue encapsulateInternal(@Nonnull final List<? extends Typed> arguments,
                                                     @Nullable FrameSpecification frameSpecification,
                                                     @Nonnull final ImmutableList<OrderingPart.RequestedOrderingPart> providedOrderingParts) {
            SemanticException.check(arguments.size() <= 1,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);

            if (frameSpecification == null) {
                frameSpecification = FrameSpecification.defaultSpecification();
            }

            if (arguments.isEmpty()) {
                return new RankValue(ImmutableList.of(), providedOrderingParts, frameSpecification);
            }
            SemanticException.check(arguments.get(0) instanceof AbstractArrayConstructorValue,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            final var partitioningValuesList = (AbstractArrayConstructorValue)arguments.get(0);

            return new RankValue(partitioningValuesList.getChildren(), providedOrderingParts, frameSpecification);
        }
    }
}
