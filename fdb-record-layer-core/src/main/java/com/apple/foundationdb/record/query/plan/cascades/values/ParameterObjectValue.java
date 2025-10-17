/*
 * QuantifiedObjectValue.java
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
import com.apple.foundationdb.record.planprotos.PParameterObjectValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A value representing the quantifier as an object. For example, this is used to represent non-nested repeated fields.
 */
@API(API.Status.EXPERIMENTAL)
public class ParameterObjectValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Parameter-Object-Value");

    @Nonnull
    private final String parameterName;
    @Nonnull
    private final Type resultType;

    private ParameterObjectValue(@Nonnull final String parameterName, @Nonnull final Type resultType) {
        this.parameterName = parameterName;
        this.resultType = resultType;
    }

    @Nonnull
    public String getParameterName() {
        return parameterName;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return this;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return context.getBinding(parameterName);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        Verify.verify(Iterables.isEmpty(explainSuppliers));
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword("$").addIdentifier(parameterName));
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

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return LeafValue.super.equalsWithoutChildren(other)
                .filter(ignored -> getParameterName().equals(((ParameterObjectValue)other).getParameterName()));
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return false;
    }

    @Nonnull
    @Override
    public Value with(@Nonnull final Type type) {
        return ParameterObjectValue.of(parameterName, type);
    }

    @Nonnull
    @Override
    public PParameterObjectValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PParameterObjectValue.Builder builder = PParameterObjectValue.newBuilder();
        builder.setParameterName(parameterName);
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setParameterObjectValue(specificValueProto).build();
    }

    @Nonnull
    public static ParameterObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PParameterObjectValue parameterObjectValueProto) {
        return new ParameterObjectValue(Objects.requireNonNull(parameterObjectValueProto.getParameterName()),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(parameterObjectValueProto.getResultType())));
    }

    @Nonnull
    public static ParameterObjectValue of(@Nonnull final String parameterName, @Nonnull final Type resultType) {
        return new ParameterObjectValue(parameterName, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PParameterObjectValue, ParameterObjectValue> {
        @Nonnull
        @Override
        public Class<PParameterObjectValue> getProtoMessageClass() {
            return PParameterObjectValue.class;
        }

        @Nonnull
        @Override
        public ParameterObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PParameterObjectValue parameterObjectValueProto) {
            return ParameterObjectValue.fromProto(serializationContext, parameterObjectValueProto);
        }
    }
}
