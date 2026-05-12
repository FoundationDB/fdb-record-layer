/*
 * RowNumberValue.java
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
import com.apple.foundationdb.record.planprotos.PRowNumberValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A leaf value representing a row number provided by an index scan. This value is index-only
 * and cannot be evaluated outside an index context.
 */
@API(API.Status.EXPERIMENTAL)
public class RowNumberValue extends AbstractValue implements LeafValue, Value.IndexOnlyValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("RowNumberValue");

    private static final RowNumberValue INSTANCE = new RowNumberValue();

    private RowNumberValue() {
    }

    @SuppressWarnings("unused")
    public RowNumberValue(@Nonnull final PlanSerializationContext serializationContext,
                          @Nonnull final PRowNumberValue proto) {
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("row_number"));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return BASE_HASH.planHash(mode);
    }

    @Nonnull
    @Override
    public PRowNumberValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRowNumberValue.newBuilder().build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRowNumberIndexValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RowNumberValue instance() {
        return INSTANCE;
    }

    @Nonnull
    public static RowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PRowNumberValue proto) {
        return INSTANCE;
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRowNumberValue, RowNumberValue> {
        @Nonnull
        @Override
        public Class<PRowNumberValue> getProtoMessageClass() {
            return PRowNumberValue.class;
        }

        @Nonnull
        @Override
        public RowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PRowNumberValue proto) {
            return RowNumberValue.fromProto(serializationContext, proto);
        }
    }
}
