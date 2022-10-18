/*
 * PromoteValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueWithChild;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Function;

/**
 * A value that flips the output of its boolean child.
 */
@API(API.Status.EXPERIMENTAL)
public class PromoteValue implements ValueWithChild {
    // This promotion map is defined based on the basic SQL promotion rules for standard SQL data types when
    // applied to our data model
    private static final Map<Pair<Type.TypeCode, Type.TypeCode>, Function<Object, Object>> PROMOTION_MAP =
            ImmutableMap.of(Pair.of(Type.TypeCode.INT, Type.TypeCode.LONG), in -> Long.valueOf((Integer)in),
                    Pair.of(Type.TypeCode.INT, Type.TypeCode.FLOAT), in -> Float.valueOf((Integer)in),
                    Pair.of(Type.TypeCode.INT, Type.TypeCode.DOUBLE), in -> Double.valueOf((Integer)in),
                    Pair.of(Type.TypeCode.LONG, Type.TypeCode.FLOAT), in -> Float.valueOf((Long)in),
                    Pair.of(Type.TypeCode.LONG, Type.TypeCode.DOUBLE), in -> Double.valueOf((Long)in),
                    Pair.of(Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE), in -> Double.valueOf((Float)in));
    /**
     * The hash value of this expression.
     */
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Promote-Value");

    /**
     * The child expression.
     */
    @Nonnull
    private final Value inValue;

    /**
     * The type that {@code inValue} should be promoted to.
     */
    @Nonnull
    private final Type promoteToType;

    @Nonnull
    private final Function<Object, Object> promotionFunction;

    /**
     * Constructs a new {@link PromoteValue} instance.
     * @param inValue The child expression.
     */
    public PromoteValue(@Nonnull final Value inValue, @Nonnull final Type promoteToType, @Nonnull final Function<Object, Object> promotionFunction) {
        this.inValue = inValue;
        this.promoteToType = promoteToType;
        this.promotionFunction = promotionFunction;
    }

    @Nonnull
    @Override
    public Value getChild() {
        return inValue;
    }

    @Nonnull
    @Override
    public PromoteValue withNewChild(@Nonnull final Value newChild) {
        return new PromoteValue(inValue, promoteToType, promotionFunction);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final Object result = inValue.eval(store, context);
        if (result == null) {
            return null;
        }
        return promotionFunction.apply(result);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, promoteToType);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, inValue, promoteToType);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "promote(" + inValue.explain(formatter) + " as " + promoteToType + ")";
    }

    @Override
    public String toString() {
        return "promote(" + inValue + " as " + promoteToType + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Nonnull
    public static Value injectIfNecessary(@Nonnull final Value inValue, @Nonnull final Type promoteToType) {
        final var inType = inValue.getResultType();
        SemanticException.check(typesCanBePromoted(inType, promoteToType), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        if (!isPromotionNeeded(inType, promoteToType)) {
            return inValue;
        }
        final var promotionFunction = resolvePromotionFunction(inType, promoteToType);
        SemanticException.check(promotionFunction != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return new PromoteValue(inValue, promoteToType, promotionFunction);
    }

    @Nullable
    private static Function<Object, Object> resolvePromotionFunction(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        return PROMOTION_MAP.get(Pair.of(inType.getTypeCode(), promoteToType.getTypeCode()));
    }

    private static boolean typesCanBePromoted(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        if (!inType.isPrimitive() || !promoteToType.isPrimitive()) {
            return false;
        }
        return inType.isNumeric() == promoteToType.isNumeric();
    }

    private static boolean isPromotionNeeded(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        return inType.getTypeCode() != promoteToType.getTypeCode();
    }
}
