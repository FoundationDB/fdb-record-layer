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
import com.apple.foundationdb.record.planprotos.PQuantifiedObjectValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A value representing the quantifier as an object. For example, this is used to represent non-nested repeated fields.
 */
@API(API.Status.EXPERIMENTAL)
public class QuantifiedObjectValue extends AbstractValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Quantified-Object-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final Type resultType;

    private QuantifiedObjectValue(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
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
        return QuantifiedObjectValue.of(targetAlias, resultType);
    }

    @Nonnull
    @Override
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return fieldValue;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        // TODO this "if" can be encoded in encapsulation code implementing type promotion rules
        final var binding = (QueryResult)context.getBinding(alias);
        if (resultType.isRecord()) {
            return binding.getDatum() == null ? null : binding.getMessage();
        } else {
            return binding.getDatum();
        }
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

    @Nonnull
    @Override
    public Map<Value, Value> pullUp(@Nonnull final Iterable<? extends Value> toBePulledUpValues,
                                    @Nonnull final AliasMap aliasMap,
                                    @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                    @Nonnull final CorrelationIdentifier upperBaseAlias) {
        final var areSimpleReferences =
                Streams.stream(toBePulledUpValues)
                        .flatMap(toBePulledUpValue -> toBePulledUpValue.getCorrelatedTo().stream())
                        .noneMatch(a -> !alias.equals(a) && constantAliases.contains(a));
        if (areSimpleReferences) {
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(alias, upperBaseAlias));
            final var translatedMapBuilder = ImmutableMap.<Value, Value>builder();
            for (final var toBePulledUpValue : toBePulledUpValues) {
                translatedMapBuilder.put(toBePulledUpValue, toBePulledUpValue.translateCorrelations(translationMap));
            }
            return translatedMapBuilder.build();
        }

        return super.pullUp(toBePulledUpValues, aliasMap, constantAliases, upperBaseAlias);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return formatter.getQuantifierName(alias);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Override
    public String toString() {
        return alias.equals(Quantifier.current()) ? "_" : "$" + alias;
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
        if (otherValue instanceof QuantifiedObjectValue) {
            return getAlias().equals(((QuantifiedObjectValue)otherValue).getAlias());
        }
        return false;
    }

    @Nonnull
    @Override
    public Value with(@Nonnull final Type type) {
        return QuantifiedObjectValue.of(getAlias(), type);
    }

    @Nonnull
    @Override
    public PQuantifiedObjectValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        PQuantifiedObjectValue.Builder builder = PQuantifiedObjectValue.newBuilder();
        builder.setAlias(alias.getId());
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setQuantifiedObjectValue(specificValueProto).build();
    }

    @Nonnull
    public static QuantifiedObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PQuantifiedObjectValue quantifiedObjectValue) {
        return new QuantifiedObjectValue(CorrelationIdentifier.of(Objects.requireNonNull(quantifiedObjectValue.getAlias())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(quantifiedObjectValue.getResultType())));
    }

    @Nonnull
    public static QuantifiedObjectValue of(@Nonnull final Quantifier quantifier) {
        return new QuantifiedObjectValue(quantifier.getAlias(), quantifier.getFlowedObjectType());
    }

    @Nonnull
    public static QuantifiedObjectValue of(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        return new QuantifiedObjectValue(alias, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PQuantifiedObjectValue, QuantifiedObjectValue> {
        @Nonnull
        @Override
        public Class<PQuantifiedObjectValue> getProtoMessageClass() {
            return PQuantifiedObjectValue.class;
        }

        @Nonnull
        @Override
        public QuantifiedObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PQuantifiedObjectValue quantifiedObjectValueProto) {
            return QuantifiedObjectValue.fromProto(serializationContext, quantifiedObjectValueProto);
        }
    }
}
