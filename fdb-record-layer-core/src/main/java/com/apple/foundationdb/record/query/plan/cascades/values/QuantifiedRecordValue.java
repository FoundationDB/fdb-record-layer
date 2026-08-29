/*
 * QuantifiedRecordValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
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
public class QuantifiedRecordValue extends AbstractValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Quantified-Record-Value");

    private final CorrelationIdentifier alias;
    private final Type resultType;

    private QuantifiedRecordValue(final CorrelationIdentifier alias, final Type resultType) {
        this.alias = alias;
        this.resultType = resultType;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Override
    public Value rebaseLeaf(final CorrelationIdentifier targetAlias) {
        return QuantifiedRecordValue.of(targetAlias, resultType);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        final var binding = (QueryResult)context.getBinding(Bindings.Internal.CORRELATION, alias);
        return binding.getQueriedRecord();
    }

    @Override
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return QuantifiedValue.super.getCorrelatedToWithoutChildren();
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
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addOpeningSquareBracket().addOptionalWhitespace()
                .addAliasReference(alias).addOptionalWhitespace().addClosingSquareBracket());
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
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        if (otherValue instanceof QuantifiedRecordValue) {
            return getAlias().equals(((QuantifiedRecordValue)otherValue).getAlias());
        }
        return false;
    }

    @Override
    public Value with(final Type type) {
        return QuantifiedRecordValue.of(getAlias(), type);
    }

    @Override
    public PQuantifiedRecordValue toProto(final PlanSerializationContext serializationContext) {
        PQuantifiedRecordValue.Builder builder = PQuantifiedRecordValue.newBuilder();
        builder.setAlias(alias.getId());
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setQuantifiedRecordValue(specificValueProto).build();
    }

    public static QuantifiedRecordValue fromProto(final PlanSerializationContext serializationContext,
                                                  final PQuantifiedRecordValue quantifiedRecordValueProto) {
        return new QuantifiedRecordValue(CorrelationIdentifier.of(Objects.requireNonNull(quantifiedRecordValueProto.getAlias())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(quantifiedRecordValueProto.getResultType())));
    }

    public static QuantifiedRecordValue of(final Quantifier quantifier) {
        return new QuantifiedRecordValue(quantifier.getAlias(), quantifier.getFlowedObjectType());
    }

    public static QuantifiedRecordValue of(final CorrelationIdentifier alias, final Type resultType) {
        return new QuantifiedRecordValue(alias, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PQuantifiedRecordValue, QuantifiedRecordValue> {
        @Override
        public Class<PQuantifiedRecordValue> getProtoMessageClass() {
            return PQuantifiedRecordValue.class;
        }

        @Override
        public QuantifiedRecordValue fromProto(final PlanSerializationContext serializationContext,
                                               final PQuantifiedRecordValue quantifiedObjectValueProto) {
            return QuantifiedRecordValue.fromProto(serializationContext, quantifiedObjectValueProto);
        }
    }
}
