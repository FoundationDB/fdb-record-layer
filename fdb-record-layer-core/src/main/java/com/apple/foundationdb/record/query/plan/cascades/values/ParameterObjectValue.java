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

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A value representing the quantifier as an object. For example, this is used to represent non-nested repeated fields.
 */
@API(API.Status.EXPERIMENTAL)
public class ParameterObjectValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Parameter-Object-Value");

    private final String parameterName;
    private final Type resultType;

    private ParameterObjectValue(final String parameterName, final Type resultType) {
        this.parameterName = parameterName;
        this.resultType = resultType;
    }

    public String getParameterName() {
        return parameterName;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Override
    public Value rebaseLeaf(final CorrelationIdentifier targetAlias) {
        return this;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        return context.getBinding(parameterName);
    }

    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
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

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return LeafValue.super.equalsWithoutChildren(other)
                .filter(ignored -> getParameterName().equals(((ParameterObjectValue)other).getParameterName()));
    }

    @Override
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        return false;
    }

    @Override
    public Value with(final Type type) {
        return ParameterObjectValue.of(parameterName, type);
    }

    @Override
    public PParameterObjectValue toProto(final PlanSerializationContext serializationContext) {
        final PParameterObjectValue.Builder builder = PParameterObjectValue.newBuilder();
        builder.setParameterName(parameterName);
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setParameterObjectValue(specificValueProto).build();
    }

    public static ParameterObjectValue fromProto(final PlanSerializationContext serializationContext,
                                                 final PParameterObjectValue parameterObjectValueProto) {
        return new ParameterObjectValue(Objects.requireNonNull(parameterObjectValueProto.getParameterName()),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(parameterObjectValueProto.getResultType())));
    }

    public static ParameterObjectValue of(final String parameterName, final Type resultType) {
        return new ParameterObjectValue(parameterName, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PParameterObjectValue, ParameterObjectValue> {
        @Override
        public Class<PParameterObjectValue> getProtoMessageClass() {
            return PParameterObjectValue.class;
        }

        @Override
        public ParameterObjectValue fromProto(final PlanSerializationContext serializationContext,
                                              final PParameterObjectValue parameterObjectValueProto) {
            return ParameterObjectValue.fromProto(serializationContext, parameterObjectValueProto);
        }
    }
}
