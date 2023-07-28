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
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueWithChild;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * A value that promotes an object of a type to an object of another type. Promotions in general agree with
 * promotions according to the SQL standard.
 */
@API(API.Status.EXPERIMENTAL)
public class PromoteValue extends AbstractValue implements ValueWithChild, Value.RangeMatchableValue {
    // This promotion map is defined based on the basic SQL promotion rules for standard SQL data types when
    // applied to our data model
    private static final Map<Pair<Type.TypeCode, Type.TypeCode>, BiFunction<Descriptors.Descriptor, Object, Object>> PROMOTION_MAP =
            ImmutableMap.<Pair<Type.TypeCode, Type.TypeCode>, BiFunction<Descriptors.Descriptor, Object, Object>>builder()
                    .put(Pair.of(Type.TypeCode.INT, Type.TypeCode.LONG), (descriptor, in) -> Long.valueOf((Integer)in))
                    .put(Pair.of(Type.TypeCode.INT, Type.TypeCode.FLOAT), (descriptor, in) -> Float.valueOf((Integer)in))
                    .put(Pair.of(Type.TypeCode.INT, Type.TypeCode.DOUBLE), (descriptor, in) -> Double.valueOf((Integer)in))
                    .put(Pair.of(Type.TypeCode.LONG, Type.TypeCode.FLOAT), (descriptor, in) -> Float.valueOf((Long)in))
                    .put(Pair.of(Type.TypeCode.LONG, Type.TypeCode.DOUBLE), (descriptor, in) -> Double.valueOf((Long)in))
                    .put(Pair.of(Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE), (descriptor, in) -> Double.valueOf((Float)in))
                    .put(Pair.of(Type.TypeCode.NULL, Type.TypeCode.DOUBLE), (descriptor, in) -> (Double) null)
                    .put(Pair.of(Type.TypeCode.NULL, Type.TypeCode.FLOAT), (descriptor, in) -> (Float) null)
                    .put(Pair.of(Type.TypeCode.NULL, Type.TypeCode.LONG), (descriptor, in) -> (Long) null)
                    .put(Pair.of(Type.TypeCode.NULL, Type.TypeCode.INT), (descriptor, in) -> (Integer) null)
                    .put(Pair.of(Type.TypeCode.NULL, Type.TypeCode.BOOLEAN), (descriptor, in) -> (Boolean) null)
                    .put(Pair.of(Type.TypeCode.NULL, Type.TypeCode.STRING), (descriptor, in) -> (String) null)
                    .build();
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

    @Nullable
    private final MessageHelpers.CoercionTrieNode promotionTrie;

    private final boolean isSimplePromotion;

    /**
     * Constructs a new {@link PromoteValue} instance. Note that the actual promotion that is carried out is viewed
     * as a <em>treatment</em> of an object of a type as an object of another type without loss of information. On the
     * Java level, the promotion carried out usually implies the conversion of an object to another object (of another
     * java type).
     * @param inValue the child expression
     * @param promoteToType the type to promote to
     * @param promotionTrie the promotion trie defining the actual promotion of the object
     */
    public PromoteValue(@Nonnull final Value inValue, @Nonnull final Type promoteToType, @Nullable final MessageHelpers.CoercionTrieNode promotionTrie) {
        this.inValue = inValue;
        this.promoteToType = promoteToType;
        this.promotionTrie = promotionTrie;
        this.isSimplePromotion = promoteToType.isPrimitive() ||
                                 (promoteToType instanceof Type.Array &&
                                  Objects.requireNonNull(((Type.Array)promoteToType).getElementType()).isPrimitive());
    }

    @Nonnull
    @Override
    public Value getChild() {
        return inValue;
    }

    @Nonnull
    @Override
    public PromoteValue withNewChild(@Nonnull final Value newChild) {
        return new PromoteValue(inValue, promoteToType, promotionTrie);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final Object result = inValue.eval(store, context);
        if (result == null) {
            return null;
        }

        if (promotionTrie == null) {
            return result;
        }

        return MessageHelpers.coerceObject(promotionTrie,
                promoteToType,
                isSimplePromotion ? null : context.getTypeRepository().getMessageDescriptor(promoteToType),
                inValue.getResultType(),
                result);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return promoteToType;
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

    @Nullable
    public static MessageHelpers.CoercionTrieNode computePromotionsTrie(@Nonnull final Type targetType,
                                                                        @Nonnull Type currentType,
                                                                        @Nullable final MessageHelpers.TransformationTrieNode transformationsTrie) {
        if (transformationsTrie != null && transformationsTrie.getValue() != null) {
            currentType = transformationsTrie.getValue().getResultType();
        }

        if (currentType.isPrimitive()) {
            SemanticException.check(targetType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            if (!isPromotionNeeded(currentType, targetType)) {
                return null;
            }
            // this is definitely a leaf; and we need to promote
            final var promotionFunction = resolvePromotionFunction(currentType, targetType);
            SemanticException.check(promotionFunction != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return new MessageHelpers.CoercionTrieNode(promotionFunction, null);
        }

        Verify.verify(targetType.getTypeCode() == currentType.getTypeCode());

        if (currentType.isArray()) {
            final var targetArrayType = (Type.Array)targetType;
            final var currentArrayType = (Type.Array)currentType;
            final var targetElementType = Verify.verifyNotNull(targetArrayType.getElementType());
            final var currentElementType = Verify.verifyNotNull(currentArrayType.getElementType());
            final var elementsTrie = computePromotionsTrie(targetElementType, currentElementType, null);
            if (elementsTrie == null && currentType.isNullable() == targetType.isNullable()) {
                return null;
            }
            return new MessageHelpers.CoercionTrieNode(arrayCoercionFunction(targetArrayType, currentArrayType, elementsTrie),
                    elementsTrie == null ? null : ImmutableMap.of(-1, elementsTrie));
        }

        Verify.verify(currentType.isRecord());
        final var targetRecordType = (Type.Record)targetType;
        final var currentRecordType = (Type.Record)currentType;
        SemanticException.check(targetRecordType.getFields().size() == currentRecordType.getFields().size(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

        final var targetFields = targetRecordType.getFields();
        final var currentFields = currentRecordType.getFields();

        final var transformationsChildrenMap = transformationsTrie == null ? null : transformationsTrie.getChildrenMap();
        final var childrenMapBuilder = ImmutableMap.<Integer, MessageHelpers.CoercionTrieNode>builder();
        for (int i = 0; i < targetFields.size(); i++) {
            final var targetField = targetFields.get(i);
            final var currentField = currentFields.get(i);

            final var transformationsFieldTrie = transformationsChildrenMap == null ? null : transformationsChildrenMap.get(i);
            final var fieldTrie = computePromotionsTrie(targetField.getFieldType(), currentField.getFieldType(), transformationsFieldTrie);
            if (fieldTrie != null) {
                childrenMapBuilder.put(i, fieldTrie);
            }
        }
        final var childrenMap = childrenMapBuilder.build();
        return childrenMap.isEmpty() ? null : new MessageHelpers.CoercionTrieNode(null, childrenMap);
    }

    @Nonnull
    private static BiFunction<Descriptors.Descriptor, Object, Object> arrayCoercionFunction(@Nonnull final Type.Array targetArrayType,
                                                                                            @Nonnull final Type.Array currentArrayType,
                                                                                            @Nullable final MessageHelpers.CoercionTrieNode elementsTrie) {
        return (targetDescriptor, current) -> MessageHelpers.coerceArray(targetArrayType, currentArrayType, targetDescriptor, elementsTrie, current);
    }

    @Nonnull
    public static Value inject(@Nonnull final Value inValue, @Nonnull final Type promoteToType) {
        final var inType = inValue.getResultType();
        if (inType.equals(promoteToType)) {
            return inValue;
        }
        final var promotionTrie = computePromotionsTrie(promoteToType, inType, null);
        return new PromoteValue(inValue, promoteToType, promotionTrie);
    }

    @Nullable
    public static BiFunction<Descriptors.Descriptor, Object, Object> resolvePromotionFunction(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        return PROMOTION_MAP.get(Pair.of(inType.getTypeCode(), promoteToType.getTypeCode()));
    }

    public static boolean isPromotionNeeded(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        SemanticException.check(inType.isPrimitive() && promoteToType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return inType.getTypeCode() != promoteToType.getTypeCode();
    }
}
