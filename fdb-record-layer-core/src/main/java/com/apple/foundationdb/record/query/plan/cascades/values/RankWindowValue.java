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
import com.apple.foundationdb.record.planprotos.PRankWindowValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A windowed value that computes the RANK of a list of expressions which can optionally be partitioned by expressions
 * defining a window.
 */
@API(API.Status.EXPERIMENTAL)
public class RankWindowValue extends WindowValue implements Value.IndexOnlyValue {
    private static final String NAME = "RANK_WINDOW";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public RankWindowValue(@Nonnull final PlanSerializationContext serializationContext,
                           @Nonnull final PRankWindowValue rankWindowValueProto) {
        super(serializationContext, Objects.requireNonNull(rankWindowValueProto.getSuper()));
    }

    public RankWindowValue(@Nonnull Iterable<? extends Value> argumentValues,
                           @Nonnull Iterable<? extends Value> partitioningValues) {
        super(argumentValues, partitioningValues);
    }

    public RankWindowValue(@Nonnull final Iterable<? extends Value> argumentValues,
                           @Nonnull final Iterable<? extends Value> partitioningValues,
                           @Nonnull final Iterable<WindowOrderingPart> orderingParts,
                           @Nonnull final FrameSpecification frameSpecification) {
        super(argumentValues, partitioningValues, orderingParts, frameSpecification);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public RankWindowValue withOrderingParts(final @Nonnull List<WindowOrderingPart> newOrderingParts) {
        return new RankWindowValue(getArgumentValues(), getPartitioningValues(), newOrderingParts, getWindowFrameSpecification());
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
    public RankWindowValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new RankWindowValue(childrenPair.getValue(), childrenPair.getKey(), splitNewOrderingParts(newChildren), getWindowFrameSpecification());
    }

    @Nonnull
    @Override
    public PRankWindowValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRankWindowValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRankValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RankWindowValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PRankWindowValue rankValueProto) {
        return new RankWindowValue(serializationContext, rankValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRankWindowValue, RankWindowValue> {
        @Nonnull
        @Override
        public Class<PRankWindowValue> getProtoMessageClass() {
            return PRankWindowValue.class;
        }

        @Nonnull
        @Override
        public RankWindowValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PRankWindowValue rankValueProto) {
            return RankWindowValue.fromProto(serializationContext, rankValueProto);
        }
    }
}
