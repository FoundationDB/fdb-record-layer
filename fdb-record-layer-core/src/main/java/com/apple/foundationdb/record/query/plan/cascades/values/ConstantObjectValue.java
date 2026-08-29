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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PConstantObjectValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Represents a constant value that references a constant in __CONST__ binding of {@link EvaluationContext}.
 */
public class ConstantObjectValue extends AbstractValue implements LeafValue, Value.RangeMatchableValue, CreatesDynamicTypesValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Constant-Object-Value");

    private final CorrelationIdentifier alias;

    private final String constantId;

    private final Type resultType;

    private ConstantObjectValue(final CorrelationIdentifier alias, final String constantId,
                                final Type resultType) {
        this.alias = alias;
        this.constantId = constantId;
        this.resultType = resultType;
    }

    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> constantId.equals(((ConstantObjectValue)other).constantId));
    }

    @Override
    public boolean canResultInType(final Type type) {
        return resultType.isNull() || (resultType.isNullable() && resultType.equals(type.nullable()));
    }

    @Override
    public Value with(final Type type) {
        if (getResultType().equals(type)) {
            return this;
        }
        Verify.verify(canResultInType(type));
        return ConstantObjectValue.of(alias, constantId, type);
    }

    public String getConstantId() {
        return constantId;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        final var obj = context.dereferenceConstant(alias, constantId);
        if (obj == null) {
            Verify.verify(getResultType().isNullable());
            return null;
        }
        if (getResultType().isRelation()) {
            return obj;
        }
        final var objType = Type.fromObject(obj);
        final var promotionNeeded = PromoteValue.isPromotionNeeded(objType, getResultType());
        if (!promotionNeeded) {
            return obj;
        }
        final var promotionOperator = PromoteValue.resolvePhysicalOperator(objType, getResultType());
        SemanticException.check(promotionOperator != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return promotionOperator.apply(null, obj);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode) {
            case VC0:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH);
            case VC1:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, constantId);
            default:
                // LEGACY
                throw new RecordCoreException("unsupported plan hash mode");
        }
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addIdentifier("@" + constantId));
    }

    @Override
    public PConstantObjectValue toProto(final PlanSerializationContext serializationContext) {
        return PConstantObjectValue.newBuilder()
                .setAlias(alias.getId())
                .setConstantId(constantId)
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setConstantObjectValue(toProto(serializationContext)).build();
    }

    public static ConstantObjectValue fromProto(final PlanSerializationContext serializationContext,
                                                final PConstantObjectValue constantObjectValueProto) {
        return new ConstantObjectValue(CorrelationIdentifier.of(Objects.requireNonNull(constantObjectValueProto.getAlias())),
                Objects.requireNonNull(constantObjectValueProto.getConstantId()),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(constantObjectValueProto.getResultType())));
    }

    /**
     * Creates a new instance of {@link ConstantObjectValue}.
     * @param alias the alias, must be a {@code CONSTANT} quantifier
     * @param constantId the id through which we can reference the constant
     * @param resultType the result of the object referenced by this constant reference
     * @return a new instance of {@link ConstantObjectValue}
     */
    public static ConstantObjectValue of(final CorrelationIdentifier alias, final String constantId,
                                         final Type resultType) {
        return new ConstantObjectValue(alias, constantId, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PConstantObjectValue, ConstantObjectValue> {
        @Override
        public Class<PConstantObjectValue> getProtoMessageClass() {
            return PConstantObjectValue.class;
        }

        @Override
        public ConstantObjectValue fromProto(final PlanSerializationContext serializationContext,
                                             final PConstantObjectValue constantObjectValueProto) {
            return ConstantObjectValue.fromProto(serializationContext, constantObjectValueProto);
        }
    }
}
