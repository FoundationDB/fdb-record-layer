/*
 * IndexedValue.java
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
import com.apple.foundationdb.record.planprotos.PIndexedValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value representing the source of a value derivation.
 */
@API(API.Status.EXPERIMENTAL)
public class IndexedValue extends AbstractValue implements LeafValue, Value.NonEvaluableValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Indexed-Value");

    private final Type resultType;

    public IndexedValue() {
        this(Type.primitiveType(Type.TypeCode.UNKNOWN));
    }

    public IndexedValue(final Type resultType) {
        this.resultType = resultType;
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Override
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        return false;
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
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("indexed"));
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
    public PIndexedValue toProto(final PlanSerializationContext serializationContext) {
        return PIndexedValue.newBuilder().setResultType(resultType.toTypeProto(serializationContext)).build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setIndexedValue(toProto(serializationContext)).build();
    }

    public static IndexedValue fromProto(final PlanSerializationContext serializationContext,
                                         final PIndexedValue indexedValueProto) {
        return new IndexedValue(Type.fromTypeProto(serializationContext,
                Objects.requireNonNull(indexedValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PIndexedValue, IndexedValue> {
        @Override
        public Class<PIndexedValue> getProtoMessageClass() {
            return PIndexedValue.class;
        }

        @Override
        public IndexedValue fromProto(final PlanSerializationContext serializationContext,
                                      final PIndexedValue indexedValueProto) {
            return IndexedValue.fromProto(serializationContext, indexedValueProto);
        }
    }
}
