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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRankValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value representing a rank computation provided by an index scan. This value is index-only
 * and cannot be evaluated outside an index context. Unlike {@link RowNumberValue}, it takes
 * argument values that define what is being ranked.
 */
@API(API.Status.EXPERIMENTAL)
public class RankValue extends WindowValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("RankValue");

    @Nonnull
    private final List<? extends Value> argumentValues;

    public RankValue(@Nonnull final WindowFrameSpecification frameSpecification,
                     @Nonnull final Iterable<? extends Value> argumentValues) {
        super(frameSpecification);
        this.argumentValues = ImmutableList.copyOf(argumentValues);
    }

    public RankValue(@Nonnull final PlanSerializationContext serializationContext,
                     @Nonnull final PRankValue proto) {
        super(serializationContext, Objects.requireNonNull(proto.getSuper()));
        this.argumentValues = proto.getArgumentValuesList().stream()
                .map(pValue -> Value.fromValueProto(serializationContext, pValue))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return argumentValues;
    }

    @Nonnull
    @Override
    public RankValue withChildren(final Iterable<? extends Value> newChildren) {
        return new RankValue(getWindowFrameSpecification(), newChildren);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("rank"));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, argumentValues);
    }

    @Nonnull
    @Override
    public PRankValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PRankValue.newBuilder()
                .setSuper(toWindowValueProto(serializationContext));
        for (final Value argumentValue : argumentValues) {
            builder.addArgumentValues(argumentValue.toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRankIndexValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RankValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PRankValue proto) {
        return new RankValue(serializationContext, proto);
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
                                   @Nonnull final PRankValue proto) {
            return RankValue.fromProto(serializationContext, proto);
        }
    }
}
