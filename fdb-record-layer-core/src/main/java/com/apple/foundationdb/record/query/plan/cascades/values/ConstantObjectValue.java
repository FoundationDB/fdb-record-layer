/*
 * ConstantObjectValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Verify;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * Represents a constant value that references a constant in __CONST__ binding of {@link EvaluationContext}.
 */
public class ConstantObjectValue extends AbstractValue implements LeafValue, Value.RangeMatchableValue {

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Constant-Object-Value");

    @Nonnull
    private final CorrelationIdentifier alias;

    private final int ordinal;

    @Nonnull
    private final Type resultType;

    private ConstantObjectValue(@Nonnull final CorrelationIdentifier alias, int ordinal, @Nonnull final Type resultType) {
        this.alias = alias;
        this.ordinal = ordinal;
        this.resultType = resultType;
    }

    @Nonnull
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Override
    @Nonnull
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap aliasMap) {
        if (!super.equalsWithoutChildren(other, aliasMap)) {
            return false;
        }

        final var otherConstantObjectValue = (ConstantObjectValue)other;
        return ordinal == otherConstantObjectValue.ordinal;
    }

    @Override
    public boolean canResultInType(@Nonnull final Type type) {
        return resultType.getTypeCode() == Type.TypeCode.NULL;
    }

    @Nonnull
    @Override
    public Value with(@Nonnull final Type type) {
        Verify.verify(canResultInType(type));
        return ConstantObjectValue.of(alias, ordinal, type);
    }

    public int getOrdinal() {
        return ordinal;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var obj = context.dereferenceConstant(alias, ordinal);
        if (getResultType().isUnresolved()) {
            return obj;
        }
        if (obj == null) {
            SemanticException.check(resultType.isNullable(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return obj;
        }
        if (obj instanceof DynamicMessage) {
            SemanticException.check(resultType.isRecord(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return obj;
        }
        final var objType = Type.fromObject(obj);
        final var promotionNeeded = PromoteValue.isPromotionNeeded(objType, getResultType());
        if (!promotionNeeded) {
            return obj;
        }
        final var promotionFunction = PromoteValue.resolvePromotionFunction(objType, getResultType());
        SemanticException.check(promotionFunction != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return promotionFunction.apply(null, obj);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, ordinal);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return toString();
    }

    @Override
    public String toString() {
        return "@" + ordinal;
    }

    /**
     * Creates a new instance of {@link ConstantObjectValue}.
     * @param alias The alias, must be a {@code CONSTANT} quantifier.
     * @param ordinal The ordinal, i.e. the offset within the {@code Object} array of constant values.
     * @param resultType The result of the object referenced by this constant reference.
     * @return A new instance of {@link ConstantObjectValue}.
     */
    @Nonnull
    public static ConstantObjectValue of(@Nonnull final CorrelationIdentifier alias, int ordinal, @Nonnull final Type resultType) {
        return new ConstantObjectValue(alias, ordinal, resultType);
    }
}
