/*
 * RankWindowValue.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * The {@code RANK} window function: the one-based position of a row within its window, where rows that agree on the
 * window's ordering columns share a rank. The expressions whose rank is computed are the value's arguments.
 */
@API(API.Status.EXPERIMENTAL)
public final class RankWindowValue extends WindowValue {
    private static final String NAME = "rank";

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Window-Value");

    public RankWindowValue(@Nonnull final Iterable<? extends Value> argumentValues,
                           @Nonnull final WindowFrameSpecification windowFrameSpecification) {
        super(argumentValues, windowFrameSpecification);
    }

    private RankWindowValue(@Nonnull final PlanSerializationContext serializationContext,
                            @Nonnull final PRankWindowValue rankWindowValueProto) {
        super(serializationContext, Objects.requireNonNull(rankWindowValueProto.getSuper()));
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
    public RankWindowValue withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        return new RankWindowValue(newChildren, getWindowFrameSpecification());
    }

    @Nonnull
    @Override
    public PRankWindowValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRankWindowValue.newBuilder()
                .setSuper(toWindowValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRankWindowValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RankWindowValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PRankWindowValue rankWindowValueProto) {
        return new RankWindowValue(serializationContext, rankWindowValueProto);
    }

    @Override
    public String toString() {
        return getName() + "(" + getArgumentValues() + ")";
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
                                         @Nonnull final PRankWindowValue rankWindowValueProto) {
            return RankWindowValue.fromProto(serializationContext, rankWindowValueProto);
        }
    }
}
