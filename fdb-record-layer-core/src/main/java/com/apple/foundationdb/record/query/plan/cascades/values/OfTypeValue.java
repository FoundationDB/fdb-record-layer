/*
 * OfTypeValue.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Checks whether a {@link Value}'s evaluation conforms to its result type.
 */
public class OfTypeValue extends AbstractValue implements Value.RangeMatchableValue, ValueWithChild {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Of-Type-Value");

    @Nonnull
    private final Value child;

    private final Type expectedType;

    private OfTypeValue(@Nonnull final Value child, @Nonnull final Type expectedType) {
        this.child = child;
        this.expectedType = expectedType;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, expectedType, child);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new OfTypeValue(rebasedChild, expectedType);
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings(value = {"NP_NONNULL_PARAM_VIOLATION"}, justification = "compile-time evaluations take their value from the context only")
    public Object compileTimeEval(@Nonnull final EvaluationContext context) {
        return eval(null, context);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context) {
        final var value = child.eval(store, context);
        if (value == null) {
            return expectedType.isNullable();
        }
        if (value instanceof DynamicMessage) {
            return expectedType.isRecord();
        }
        final var type = Type.fromObject(value);
        return expectedType.equals(type);
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

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, expectedType);
    }

    @Override
    public String toString() {
        return child + " ofType " + expectedType;
    }

    @Nonnull
    public static OfTypeValue of(@Nonnull final Value value, @Nonnull final Type type) {
        return new OfTypeValue(value, type);
    }

    @Nonnull
    public static OfTypeValue from(@Nonnull final ConstantObjectValue value) {
        return new OfTypeValue(ConstantObjectValue.of(value.getAlias(), value.getOrdinal(), Type.any()), value.getResultType());
    }
}
