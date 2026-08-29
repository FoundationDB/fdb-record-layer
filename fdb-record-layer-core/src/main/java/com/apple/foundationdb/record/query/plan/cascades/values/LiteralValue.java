/*
 * LiteralValue.java
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
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PLiteralValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.function.Supplier;

/**
 * A wrapper around a literal of the given type.
 * @param <T> the type of the literal
 */
@API(API.Status.EXPERIMENTAL)
public class LiteralValue<T> extends AbstractValue implements LeafValue, Value.RangeMatchableValue, PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Literal-Value");

    private final Type resultType;

    @Nullable
    private final T value;

    @VisibleForTesting
    public LiteralValue(@Nullable final T value) {
        this(Type.fromObject(value), value);
    }

    @VisibleForTesting
    public LiteralValue(Type resultType, @Nullable final T value) {
        this.resultType = resultType;
        this.value = value;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        return value;
    }

    @Nullable
    public T getLiteralValue() {
        return value;
    }

    @Override
    public boolean canResultInType(final Type type) {
        return type.isNullable() && resultType.equals(type.nullable());
    }

    @Override
    public LiteralValue<T> with(final Type type) {
        if (getResultType().equals(type)) {
            return this;
        }
        Verify.verify(canResultInType(type));
        return new LiteralValue<>(type, value);
    }

    @Override
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        return false;
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return LeafValue.super.equalsWithoutChildren(other)
                .filter(ignored -> {
                    final LiteralValue<?> that = (LiteralValue<?>)other;
                    if (value == null && that.value == null) {
                        return true;
                    }
                    return Boolean.TRUE.equals(Comparisons.evalComparison(Comparisons.Type.EQUALS, value, that.value));
                });
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, value);
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
    public PLiteralValue toProto(final PlanSerializationContext serializationContext) {
        final var builder = PLiteralValue.newBuilder();
        // TODO there should be no punishment if the type is serialized alongside with the value
        final var resultTypeProto = resultType.isVector() ? resultType.toTypeProto(serializationContext) : null;
        builder.setValue(PlanSerialization.valueObjectToProto(value, resultTypeProto));
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder()
                .setLiteralValue(specificValueProto)
                .build();
    }

    @SuppressWarnings("unchecked")
    public static <T> LiteralValue<T> fromProto(final PlanSerializationContext serializationContext,
                                                final PLiteralValue literalValueProto) {
        return new LiteralValue<>(Type.fromTypeProto(serializationContext, literalValueProto.getResultType()),
                (T)PlanSerialization.protoToValueObject(literalValueProto.getValue()));
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(
                new ExplainTokens().addToString(formatLiteral(resultType, Comparisons.toPrintable(value))));
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

    public static String formatLiteral(final Type type, final String literal) {
        final String comparandString;
        if (type.isPrimitive()) {
            switch (type.getTypeCode()) {
                case INT:
                    comparandString = literal;
                    break;
                case LONG:
                    comparandString = literal + "l";
                    break;
                case FLOAT:
                    comparandString = literal + "f";
                    break;
                case DOUBLE:
                    comparandString = literal + "d";
                    break;
                case UNKNOWN: // fallthrough
                case ANY: // fallthrough
                case BOOLEAN: // fallthrough
                case BYTES: // fallthrough
                case RECORD: // fallthrough
                case ARRAY: // fallthrough
                case RELATION: // fallthrough
                case STRING: // fallthrough
                default:
                    comparandString = "'" + literal + "'";
                    break;
            }
        } else {
            comparandString = literal;
        }
        return comparandString;
    }

    public static <T> LiteralValue<T> ofScalar(final T value) {
        final var result = new LiteralValue<>(Type.fromObject(value), value);
        Verify.verify(result.resultType.isPrimitive());
        return result;
    }

    public static <T> LiteralValue<List<T>> ofList(final List<T> listValue) {
        Type resolvedElementType = null;
        for (final var elementValue : listValue) {
            final var currentType = Type.primitiveType(Type.typeCodeFromPrimitive(elementValue));
            if (resolvedElementType == null) {
                resolvedElementType = currentType;
            } else {
                if (!currentType.equals(resolvedElementType)) {
                    resolvedElementType = new Type.Any();
                    break;
                }
            }
        }

        resolvedElementType = resolvedElementType == null
                       ? new Type.Any()
                       : resolvedElementType;
        return new LiteralValue<>(new Type.Array(resolvedElementType), listValue);
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    /**
     * Deserializer.
     * @param <T> the type of literal
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer<T> implements PlanDeserializer<PLiteralValue, LiteralValue<T>> {
        @Override
        public Class<PLiteralValue> getProtoMessageClass() {
            return PLiteralValue.class;
        }

        @Override
        public LiteralValue<T> fromProto(final PlanSerializationContext serializationContext,
                                         final PLiteralValue literalValueProto) {
            return LiteralValue.fromProto(serializationContext, literalValueProto);
        }
    }
}
