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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper around a literal of the given type.
 * @param <T> the type of the literal
 */
@API(API.Status.EXPERIMENTAL)
public class LiteralValue<T> implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Literal-Value");

    @Nonnull
    private final Type resultType;

    @Nullable
    private final T value;

    public LiteralValue(@Nullable final T value) {
        this(Type.primitiveType(typeCodeFromLiteral(value), false), value);
    }

    @VisibleForTesting
    public LiteralValue(@Nonnull Type resultType, @Nullable final T value) {
        this.resultType = resultType;
        this.value = value;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return value;
    }

    @Nullable
    public T getLiteralValue() {
        return value;
    }

    @Nonnull
    @Override
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return this;
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return false;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!LeafValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }

        final LiteralValue<?> that = (LiteralValue<?>)other;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(value);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, value);
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return formatLiteral(resultType, Comparisons.toPrintable(value));
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
    public static Type.TypeCode typeCodeFromLiteral(@Nullable final Object o) {
        return Type.getClassToTypeCodeMap().getOrDefault(o == null ? null : o.getClass(), Type.TypeCode.UNKNOWN);
    }

    @Nonnull
    public static String formatLiteral(@Nonnull final Type type, @Nonnull final String literal) {
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
        return new LiteralValue<>(value);
    }

    public static <T> LiteralValue<List<T>> ofList(final List<T> listValue) {
        Type resolvedElementType = null;
        for (final var elementValue : listValue) {
            final var currentType = Type.primitiveType(typeCodeFromLiteral(elementValue));
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
}
