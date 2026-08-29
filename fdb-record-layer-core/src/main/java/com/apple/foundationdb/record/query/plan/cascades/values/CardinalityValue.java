/*
 * CardinalityValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PCardinalityValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value representing the {@code CARDINALITY()} function.
 */
@API(API.Status.EXPERIMENTAL)
public class CardinalityValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Cardinality-Value");

    private final Value childValue;

    public CardinalityValue(final Value childValue) {
        SemanticException.check(childValue.getResultType().isArray(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE, "The argument of CARDINALITY() must be an array expression.");

        this.childValue = childValue;
    }

    @Override
    public List<? extends Value> computeChildren() {
        return List.of(childValue);
    }

    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var newChildrenList = ImmutableList.copyOf(newChildren);
        Verify.verify(newChildrenList.size() == 1);
        return new CardinalityValue(newChildrenList.get(0));
    }

    @Override
    public Type getResultType() {
        // Array indexes and sizes are 32-bit integers.
        return Type.primitiveType(Type.TypeCode.INT);
    }

    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        final Object childResult = childValue.eval(store, context);
        if (childResult == null) {
            return null;
        }
        return ((List<?>)childResult).size();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, childValue);
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall(FunctionNames.CARDINALITY,
                Value.explainFunctionArguments(explainSuppliers)));
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
    public PCardinalityValue toProto(final PlanSerializationContext serializationContext) {
        return PCardinalityValue.newBuilder()
                .setChildValue(childValue.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PValue toValueProto(PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCardinalityValue(toProto(serializationContext)).build();
    }

    public static CardinalityValue fromProto(final PlanSerializationContext serializationContext, final PCardinalityValue cardinalityValueProto) {
        return new CardinalityValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(cardinalityValueProto.getChildValue())));
    }

    /**
     * The {@code CARDINALITY()} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class CardinalityFn extends BuiltInFunction<Value> {
        public CardinalityFn() {
            super(FunctionNames.CARDINALITY,
                    List.of(Type.any()), (builtInFunction, arguments) -> encapsulateInternal(arguments.getArgumentsList()));
        }

        private static Value encapsulateInternal(final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            return new CardinalityValue((Value)arguments.get(0));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCardinalityValue, CardinalityValue> {
        @Override
        public Class<PCardinalityValue> getProtoMessageClass() {
            return PCardinalityValue.class;
        }

        @Override
        public CardinalityValue fromProto(final PlanSerializationContext serializationContext,
                                          final PCardinalityValue cardinalityValueProto) {
            return CardinalityValue.fromProto(serializationContext, cardinalityValueProto);
        }
    }
}
