/*
 * ConstantValue.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PConstantValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A wrapper around a constant.
 */
@API(API.Status.EXPERIMENTAL)
public class ConstantValue extends AbstractValue implements LeafValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Constant-Value");

    private final Value value;

    public ConstantValue(final Value value) {
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public Type getResultType() {
        return getValue().getResultType();
    }

    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        return value.eval(store, context);
    }

    @Override
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        return false;
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return LeafValue.super.equalsWithoutChildren(other)
                .filter(ignored -> value.equals(((ConstantValue)other).value));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(value);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, value);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("const",
                Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public PConstantValue toProto(final PlanSerializationContext serializationContext) {
        return PConstantValue.newBuilder()
                .setValue(value.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setConstantValue(toProto(serializationContext)).build();
    }

    public static ConstantValue fromProto(final PlanSerializationContext serializationContext,
                                          final PConstantValue constantValueProto) {
        return new ConstantValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(constantValueProto.getValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PConstantValue, ConstantValue> {
        @Override
        public Class<PConstantValue> getProtoMessageClass() {
            return PConstantValue.class;
        }

        @Override
        public ConstantValue fromProto(final PlanSerializationContext serializationContext,
                                       final PConstantValue constantValueProto) {
            return ConstantValue.fromProto(serializationContext, constantValueProto);
        }
    }
}
