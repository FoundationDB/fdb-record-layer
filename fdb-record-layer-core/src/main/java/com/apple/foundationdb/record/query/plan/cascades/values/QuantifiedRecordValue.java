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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PQuantifiedRecordValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A value representing the quantifier as an object. For example, this is used to represent non-nested repeated fields.
 */
@API(API.Status.EXPERIMENTAL)
public class QuantifiedRecordValue extends AbstractValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Quantified-Record-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final Type resultType;

    private QuantifiedRecordValue(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        this.alias = alias;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return QuantifiedRecordValue.of(targetAlias, resultType);
    }

    @Nonnull
    @Override
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return fieldValue;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var binding = (QueryResult)context.getBinding(Bindings.Internal.CORRELATION, alias);
        return binding.getQueriedRecord();
    }

    @Nonnull
    @Override
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return QuantifiedValue.super.getCorrelatedToWithoutChildren();
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
    public ExplainInfo explain(@Nonnull final Formatter formatter,
                               @Nonnull final Iterable<Function<Formatter, ExplainInfo>> explainFunctions) {
        return ExplainInfo.of("[" +
                (alias.equals(Quantifier.current()) ? "_" : formatter.getQuantifierName(alias)) + "]");
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
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        if (otherValue instanceof QuantifiedRecordValue) {
            return getAlias().equals(((QuantifiedRecordValue)otherValue).getAlias());
        }
        return false;
    }

    @Nonnull
    @Override
    public Value with(@Nonnull final Type type) {
        return QuantifiedRecordValue.of(getAlias(), type);
    }

    @Nonnull
    @Override
    public PQuantifiedRecordValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        PQuantifiedRecordValue.Builder builder = PQuantifiedRecordValue.newBuilder();
        builder.setAlias(alias.getId());
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setQuantifiedRecordValue(specificValueProto).build();
    }

    @Nonnull
    public static QuantifiedRecordValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PQuantifiedRecordValue quantifiedObjectValue) {
        return new QuantifiedRecordValue(CorrelationIdentifier.of(Objects.requireNonNull(quantifiedObjectValue.getAlias())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(quantifiedObjectValue.getResultType())));
    }

    @Nonnull
    public static QuantifiedRecordValue of(@Nonnull final Quantifier quantifier) {
        return new QuantifiedRecordValue(quantifier.getAlias(), quantifier.getFlowedObjectType());
    }

    @Nonnull
    public static QuantifiedRecordValue of(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        return new QuantifiedRecordValue(alias, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PQuantifiedRecordValue, QuantifiedRecordValue> {
        @Nonnull
        @Override
        public Class<PQuantifiedRecordValue> getProtoMessageClass() {
            return PQuantifiedRecordValue.class;
        }

        @Nonnull
        @Override
        public QuantifiedRecordValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PQuantifiedRecordValue quantifiedObjectValueProto) {
            return QuantifiedRecordValue.fromProto(serializationContext, quantifiedObjectValueProto);
        }
    }
}
