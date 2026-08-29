/*
 * DerivedValue.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PDerivedValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class DerivedValue extends AbstractValue implements Value.NonEvaluableValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Derived-Value");

    private final List<? extends Value> children;

    private final Type resultType;

    public DerivedValue(Iterable<? extends Value> values) {
        this(values, Type.primitiveType(Type.TypeCode.UNKNOWN));
    }

    public DerivedValue(Iterable<? extends Value> values, Type resultType) {
        this.children = ImmutableList.copyOf(values);
        this.resultType = resultType;
        Preconditions.checkArgument(!children.isEmpty());
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Override
    public DerivedValue withChildren(final Iterable<? extends Value> newChildren) {
        return new DerivedValue(newChildren);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, children);
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens()
                .addFunctionCall("derived", Value.explainFunctionArguments(explainSuppliers)));
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
    public PDerivedValue toProto(final PlanSerializationContext serializationContext) {
        final var builder = PDerivedValue.newBuilder();
        for (final Value child : children) {
            builder.addChildren(child.toValueProto(serializationContext));
        }
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setDerivedValue(toProto(serializationContext)).build();
    }

    public static DerivedValue fromProto(final PlanSerializationContext serializationContext,
                                         final PDerivedValue derivedValueProto) {
        final ImmutableList.Builder<Value> childrenBuilder = ImmutableList.builder();
        for (int i = 0; i < derivedValueProto.getChildrenCount(); i ++) {
            childrenBuilder.add(Value.fromValueProto(serializationContext, derivedValueProto.getChildren(i)));
        }
        return new DerivedValue(childrenBuilder.build(),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(derivedValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PDerivedValue, DerivedValue> {
        @Override
        public Class<PDerivedValue> getProtoMessageClass() {
            return PDerivedValue.class;
        }

        @Override
        public DerivedValue fromProto(final PlanSerializationContext serializationContext,
                                      final PDerivedValue derivedValueProto) {
            return DerivedValue.fromProto(serializationContext, derivedValueProto);
        }
    }
}
