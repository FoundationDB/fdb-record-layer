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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;

import java.util.Objects;

/**
 * A windowed value that computes the RANK of a list of expressions which can optionally be partitioned by expressions
 * defining a window.
 */
@API(API.Status.EXPERIMENTAL)
public class RankValue extends WindowedValue implements Value.IndexOnlyValue {
    private static final String NAME = "RANK";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public RankValue(final PlanSerializationContext serializationContext,
                     final PRankValue rankValueProto) {
        super(serializationContext, Objects.requireNonNull(rankValueProto.getSuper()));
    }

    public RankValue(Iterable<? extends Value> partitioningValues,
                     Iterable<? extends Value> argumentValues) {
        super(partitioningValues, argumentValues);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return basePlanHash(mode, BASE_HASH);
    }

    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Override
    public RankValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new RankValue(childrenPair.getKey(), childrenPair.getValue());
    }

    @Override
    public PRankValue toProto(final PlanSerializationContext serializationContext) {
        return PRankValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRankValue(toProto(serializationContext)).build();
    }

    public static RankValue fromProto(final PlanSerializationContext serializationContext,
                                      final PRankValue rankValueProto) {
        return new RankValue(serializationContext, rankValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRankValue, RankValue> {
        @Override
        public Class<PRankValue> getProtoMessageClass() {
            return PRankValue.class;
        }

        @Override
        public RankValue fromProto(final PlanSerializationContext serializationContext,
                                   final PRankValue rankValueProto) {
            return RankValue.fromProto(serializationContext, rankValueProto);
        }
    }
}
