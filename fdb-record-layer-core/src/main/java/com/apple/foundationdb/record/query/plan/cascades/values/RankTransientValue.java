/*
 * RankValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRankTransientValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A windowed value that computes the RANK of a list of expressions which can optionally be partitioned by expressions
 * defining a window.
 */
@API(API.Status.EXPERIMENTAL)
public class RankTransientValue extends TransientWindowValue implements Value.IndexOnlyValue {
    private static final String NAME = "RANK_WINDOW";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public RankTransientValue(@Nonnull final PlanSerializationContext serializationContext,
                              @Nonnull final PRankTransientValue rankWindowValueProto) {
        super(serializationContext, Objects.requireNonNull(rankWindowValueProto.getSuper()));
    }

    public RankTransientValue(@Nonnull Iterable<? extends Value> argumentValues,
                              @Nonnull Iterable<? extends Value> partitioningValues) {
        super(argumentValues, partitioningValues);
    }

    public RankTransientValue(@Nonnull final Iterable<? extends Value> argumentValues,
                              @Nonnull final Iterable<? extends Value> partitioningValues,
                              @Nonnull final Iterable<WindowOrderingPart> orderingParts,
                              @Nonnull final WindowFrameSpecification frameSpecification) {
        super(argumentValues, partitioningValues, orderingParts, frameSpecification);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public RankTransientValue withOrderingParts(final @Nonnull List<WindowOrderingPart> newOrderingParts) {
        return new RankTransientValue(getArgumentValues(), getPartitioningValues(), newOrderingParts, getWindowFrameSpecification());
    }

    @Nonnull
    @Override
    public WindowValue toWindowValue() {
        return new RankValue(getWindowFrameSpecification(), getArgumentValues());
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
    public RankTransientValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new RankTransientValue(childrenPair.getValue(), childrenPair.getKey(), splitNewOrderingParts(newChildren), getWindowFrameSpecification());
    }

    @Nonnull
    @Override
    public PRankTransientValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRankTransientValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRankValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RankTransientValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PRankTransientValue rankValueProto) {
        return new RankTransientValue(serializationContext, rankValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRankTransientValue, RankTransientValue> {
        @Nonnull
        @Override
        public Class<PRankTransientValue> getProtoMessageClass() {
            return PRankTransientValue.class;
        }

        @Nonnull
        @Override
        public RankTransientValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PRankTransientValue rankValueProto) {
            return RankTransientValue.fromProto(serializationContext, rankValueProto);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static final class RankFn extends BuiltInFunction<RankTransientValue> {
        public RankFn() {
            super("rank", ImmutableList.of(Type.any()), Type.any(), (builtInFunction, callSiteArguments) -> {
                SemanticException.check(!callSiteArguments.isNamed(), SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                final var windowSpecification = callSiteArguments.getWindowSpecification();
                return new RankTransientValue(callSiteArguments.getValues(), windowSpecification.partitioningValues(),
                        windowSpecification.orderingParts(), windowSpecification.frameSpecification());
            });
        }
    }
}
