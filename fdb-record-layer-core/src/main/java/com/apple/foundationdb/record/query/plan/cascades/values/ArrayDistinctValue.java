/*
 * ArrayDistinctValue.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PArrayDistinctValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
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
 * A value that returns a copy of its child array {@link Value} with all duplicate elements removed.
 */
@API(API.Status.EXPERIMENTAL)
public class ArrayDistinctValue extends AbstractValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Array-Distinct-Value");

    private final Value childValue;
    private final Type resultType;

    public ArrayDistinctValue(final Value childValue) {
        final var innerResultType = Objects.requireNonNull(childValue.getResultType());
        Verify.verify(innerResultType.isArray());
        this.childValue = childValue;
        this.resultType = innerResultType;
    }

    @Override
    public List<? extends Value> computeChildren() {
        return ImmutableList.of(childValue);
    }

    @Override
    public Value getChild() {
        return childValue;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ValueWithChild withNewChild(final Value rebasedChild) {
        if (getChild() == rebasedChild) {
            return this;
        }
        return new ArrayDistinctValue(rebasedChild);
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        final var childResult = childValue.eval(store, context);
        if (childResult == null) {
            return null;
        }
        return ((List<?>)childResult).stream().distinct().collect(ImmutableList.toImmutableList());
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
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("arrayDistinct",
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
    public PArrayDistinctValue toProto(final PlanSerializationContext serializationContext) {
        return PArrayDistinctValue.newBuilder()
                .setChildValue(childValue.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PValue toValueProto(PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setArrayDistinctValue(toProto(serializationContext)).build();
    }

    public static ArrayDistinctValue fromProto(final PlanSerializationContext serializationContext,
                                               final PArrayDistinctValue arrayDistinctValueProto) {
        return new ArrayDistinctValue(
                Value.fromValueProto(serializationContext, Objects.requireNonNull(arrayDistinctValueProto.getChildValue()))
        );
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PArrayDistinctValue, ArrayDistinctValue> {
        @Override
        public Class<PArrayDistinctValue> getProtoMessageClass() {
            return PArrayDistinctValue.class;
        }

        @Override
        public ArrayDistinctValue fromProto(final PlanSerializationContext serializationContext,
                                            final PArrayDistinctValue arrayDistinctValueProto) {
            return ArrayDistinctValue.fromProto(serializationContext, arrayDistinctValueProto);
        }
    }
}
