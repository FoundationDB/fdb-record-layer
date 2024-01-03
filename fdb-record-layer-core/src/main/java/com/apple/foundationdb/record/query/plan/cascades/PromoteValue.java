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
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.RecordQueryPlanProto.PArrayCoercionBiFunction;
import com.apple.foundationdb.record.RecordQueryPlanProto.PCoercionBiFunction;
import com.apple.foundationdb.record.RecordQueryPlanProto.PPrimitiveCoercionBiFunction;
import com.apple.foundationdb.record.RecordQueryPlanProto.PPrimitiveCoercionBiFunction.PPhysicalOperator;
import com.apple.foundationdb.record.RecordQueryPlanProto.PPromoteValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers.CoercionTrieNode;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueWithChild;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.annotation.ProtoMessage;
import com.google.auto.service.AutoService;
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
@AutoService(PlanSerializable.class)
@ProtoMessage(PPromoteValue.class)
public class PromoteValue extends AbstractValue implements ValueWithChild, Value.RangeMatchableValue {
    // This promotion map is defined based on the basic SQL promotion rules for standard SQL data types when
    // applied to our data model
    private enum PhysicalOperator {
        INT_TO_LONG(Type.TypeCode.INT, Type.TypeCode.LONG, (descriptor, in) -> Long.valueOf((Integer)in)),
        INT_TO_FLOAT(Type.TypeCode.INT, Type.TypeCode.FLOAT, (descriptor, in) -> Float.valueOf((Integer)in)),
        INT_TO_DOUBLE(Type.TypeCode.INT, Type.TypeCode.DOUBLE, (descriptor, in) -> Double.valueOf((Integer)in)),
        LONG_TO_FLOAT(Type.TypeCode.LONG, Type.TypeCode.FLOAT, (descriptor, in) -> Float.valueOf((Long)in)),
        LONG_TO_DOUBLE(Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (descriptor, in) -> Double.valueOf((Long)in)),
        FLOAT_TO_DOUBLE(Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (descriptor, in) -> Double.valueOf((Float)in)),
        NULL_TO_INT(Type.TypeCode.NULL, Type.TypeCode.INT, (descriptor, in) -> (Integer) null),
        NULL_TO_LONG(Type.TypeCode.NULL, Type.TypeCode.LONG, (descriptor, in) -> (Long) null),
        NULL_TO_FLOAT(Type.TypeCode.NULL, Type.TypeCode.FLOAT, (descriptor, in) -> (Float) null),
        NULL_TO_DOUBLE(Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (descriptor, in) -> (Double) null),
        NULL_TO_BOOLEAN(Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (descriptor, in) -> (Boolean) null),
        NULL_TO_STRING(Type.TypeCode.NULL, Type.TypeCode.STRING, (descriptor, in) -> (String) null),
        NULL_TO_ARRAY(Type.TypeCode.NULL, Type.TypeCode.ARRAY, (descriptor, in) -> null),
        NULL_TO_RECORD(Type.TypeCode.NULL, Type.TypeCode.RECORD, (descriptor, in) -> null);

        PhysicalOperator(@Nonnull final Type.TypeCode from, @Nonnull final Type.TypeCode to,
                         @Nonnull final BiFunction<Descriptors.Descriptor, Object, Object> promotionFunction) {
            this.from = from;
            this.to = to;
            this.promotionFunction = promotionFunction;
        }

        @Nonnull
        private final Type.TypeCode from;
        @Nonnull
        private final Type.TypeCode to;
        @Nonnull
        private final BiFunction<Descriptors.Descriptor, Object, Object> promotionFunction;

        @Nonnull
        public Type.TypeCode getFrom() {
            return from;
        }

        @Nonnull
        public Type.TypeCode getTo() {
            return to;
        }

        @Nonnull
        public BiFunction<Descriptors.Descriptor, Object, Object> getPromotionFunction() {
            return promotionFunction;
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case INT_TO_LONG:
                    return PPhysicalOperator.INT_TO_LONG;
                case INT_TO_FLOAT:
                    return PPhysicalOperator.INT_TO_FLOAT;
                case INT_TO_DOUBLE:
                    return PPhysicalOperator.INT_TO_DOUBLE;
                case LONG_TO_FLOAT:
                    return PPhysicalOperator.LONG_TO_FLOAT;
                case LONG_TO_DOUBLE:
                    return PPhysicalOperator.LONG_TO_DOUBLE;
                case FLOAT_TO_DOUBLE:
                    return PPhysicalOperator.FLOAT_TO_DOUBLE;
                case NULL_TO_INT:
                    return PPhysicalOperator.NULL_TO_INT;
                case NULL_TO_LONG:
                    return PPhysicalOperator.NULL_TO_LONG;
                case NULL_TO_FLOAT:
                    return PPhysicalOperator.NULL_TO_FLOAT;
                case NULL_TO_DOUBLE:
                    return PPhysicalOperator.NULL_TO_DOUBLE;
                case NULL_TO_BOOLEAN:
                    return PPhysicalOperator.NULL_TO_BOOLEAN;
                case NULL_TO_STRING:
                    return PPhysicalOperator.NULL_TO_STRING;
                case NULL_TO_ARRAY:
                    return PPhysicalOperator.NULL_TO_ARRAY;
                case NULL_TO_RECORD:
                    return PPhysicalOperator.NULL_TO_RECORD;
                default:
                    throw new RecordCoreException("unknown operator mapping. did you forget to add it?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static PhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PPhysicalOperator operatorProto) {
            switch (operatorProto) {
                case INT_TO_LONG:
                    return INT_TO_LONG;
                case INT_TO_FLOAT:
                    return INT_TO_FLOAT;
                case INT_TO_DOUBLE:
                    return INT_TO_DOUBLE;
                case LONG_TO_FLOAT:
                    return LONG_TO_FLOAT;
                case LONG_TO_DOUBLE:
                    return LONG_TO_DOUBLE;
                case FLOAT_TO_DOUBLE:
                    return FLOAT_TO_DOUBLE;
                case NULL_TO_INT:
                    return NULL_TO_INT;
                case NULL_TO_LONG:
                    return NULL_TO_LONG;
                case NULL_TO_FLOAT:
                    return NULL_TO_FLOAT;
                case NULL_TO_DOUBLE:
                    return NULL_TO_DOUBLE;
                case NULL_TO_BOOLEAN:
                    return NULL_TO_BOOLEAN;
                case NULL_TO_STRING:
                    return NULL_TO_STRING;
                case NULL_TO_ARRAY:
                    return NULL_TO_ARRAY;
                case NULL_TO_RECORD:
                    return NULL_TO_RECORD;
                default:
                    throw new RecordCoreException("unknown operator mapping. did you forget to add it?");
            }
        }
    }

    private static final Map<Pair<Type.TypeCode, Type.TypeCode>, PhysicalOperator> PROMOTION_MAP;

    static {
        final var mapBuilder = ImmutableMap.<Pair<Type.TypeCode, Type.TypeCode>, PhysicalOperator>builder();
        for (final var operator : PhysicalOperator.values()) {
            mapBuilder.put(Pair.of(operator.getFrom(), operator.getTo()), operator);
        }
        PROMOTION_MAP = mapBuilder.put(Pair.of(Type.TypeCode.NONE, Type.TypeCode.ARRAY), (descriptor, in) -> in)
                    .build();
    }
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
    private final CoercionTrieNode promotionTrie;

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
    public PromoteValue(@Nonnull final Value inValue, @Nonnull final Type promoteToType, @Nullable final CoercionTrieNode promotionTrie) {
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
        return new PromoteValue(newChild, promoteToType, promotionTrie);
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
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, promoteToType);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, inValue, promoteToType);
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
    @Override
    public PPromoteValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PPromoteValue.Builder builder =
                PPromoteValue.newBuilder()
                        .setInValue(inValue.toValueProto(serializationContext))
                        .setPromoteToType(promoteToType.toTypeProto(serializationContext));
        if (promotionTrie != null) {
            builder.setPromotionTrie(promotionTrie.toProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordQueryPlanProto.PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return RecordQueryPlanProto.PValue.newBuilder().setPromoteValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static PromoteValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PPromoteValue promoteValueProto) {
        final CoercionTrieNode promotionTrie;
        if (promoteValueProto.hasPromotionTrie()) {
            promotionTrie = CoercionTrieNode.fromProto(serializationContext, Objects.requireNonNull(promoteValueProto.getPromotionTrie()));
        } else {
            promotionTrie = null;
        }

        return new PromoteValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(promoteValueProto.getInValue())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(promoteValueProto.getPromoteToType())),
                promotionTrie);
    }

    @Nullable
    public static CoercionTrieNode computePromotionsTrie(@Nonnull final Type targetType,
                                                         @Nonnull Type currentType,
                                                         @Nullable final MessageHelpers.TransformationTrieNode transformationsTrie) {
        if (transformationsTrie != null && transformationsTrie.getValue() != null) {
            currentType = transformationsTrie.getValue().getResultType();
        }

        if (currentType.isPrimitive()) {
            if (!isPromotionNeeded(currentType, targetType)) {
                return null;
            }
            // this is definitely a leaf; and we need to promote
            final var physicalOperator = resolvePhysicalOperator(currentType, targetType);
            SemanticException.check(physicalOperator != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return new CoercionTrieNode(new PrimitiveCoercionBiFunction(physicalOperator), null);
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
            return new CoercionTrieNode(new ArrayCoercionBiFunction(currentArrayType, targetArrayType, elementsTrie),
                    elementsTrie == null ? null : ImmutableMap.of(-1, elementsTrie));
        }

        Verify.verify(currentType.isRecord());
        final var targetRecordType = (Type.Record)targetType;
        final var currentRecordType = (Type.Record)currentType;
        SemanticException.check(targetRecordType.getFields().size() == currentRecordType.getFields().size(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

        final var targetFields = targetRecordType.getFields();
        final var currentFields = currentRecordType.getFields();

        final var transformationsChildrenMap = transformationsTrie == null ? null : transformationsTrie.getChildrenMap();
        final var childrenMapBuilder = ImmutableMap.<Integer, CoercionTrieNode>builder();
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
        return childrenMap.isEmpty() ? null : new CoercionTrieNode(null, childrenMap);
    }

    @Nonnull
    public static Value inject(@Nonnull final Value inValue, @Nonnull final Type promoteToType) {
        final var inType = inValue.getResultType();
        if (inType.equals(promoteToType)) {
            return inValue;
        }
        if (inValue.canResultInType(promoteToType)) {
            return inValue.with(promoteToType);
        }
        final var promotionTrie = computePromotionsTrie(promoteToType, inType, null);
        return new PromoteValue(inValue, promoteToType, promotionTrie);
    }

    public static boolean isPromotable(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        return resolvePhysicalOperator(inType, promoteToType) != null;
    }

    @Nullable
    private static PhysicalOperator resolvePhysicalOperator(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        return PROMOTION_MAP.get(Pair.of(inType.getTypeCode(), promoteToType.getTypeCode()));
    }

    public static boolean isPromotionNeeded(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        if (promoteToType.getTypeCode() == Type.TypeCode.ANY) {
            return false;
        }
        if (inType.getTypeCode() == Type.TypeCode.NULL) {
            return true;
        }
        if (inType.getTypeCode() == Type.TypeCode.NONE) {
            return true;
        }
        if (inType.isArray() && promoteToType.isArray()) {
            final var inArray = (Type.Array)inType;
            final var promoteToArray = (Type.Array)promoteToType;
            SemanticException.check(!inArray.isErased() && !promoteToArray.isErased(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return isPromotionNeeded(Verify.verifyNotNull(inArray.getElementType()), Verify.verifyNotNull(promoteToArray.getElementType()));
        }
        SemanticException.check(inType.isPrimitive() && promoteToType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return inType.getTypeCode() != promoteToType.getTypeCode();
    }

    /**
     * A coercion function for primitive types.
     */
    @AutoService(PlanSerializable.class)
    @ProtoMessage(PPrimitiveCoercionBiFunction.class)
    public static class PrimitiveCoercionBiFunction implements MessageHelpers.CoercionBiFunction {
        @Nonnull
        private final PhysicalOperator operator;

        private PrimitiveCoercionBiFunction(@Nonnull final PhysicalOperator operator) {
            this.operator = operator;
        }

        @Override
        public Object apply(final Descriptors.Descriptor targetDescriptor, final Object current) {
            return operator.getPromotionFunction().apply(targetDescriptor, current);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, operator.name());
        }

        @Nonnull
        @Override
        public PPrimitiveCoercionBiFunction toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PPrimitiveCoercionBiFunction.newBuilder()
                    .setOperator(operator.toProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PCoercionBiFunction toCoercionBiFunctionProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCoercionBiFunction.newBuilder().setPrimitiveCoercionBiFunction(toProto(serializationContext)).build();
        }

        @Nonnull
        public static PrimitiveCoercionBiFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                            @Nonnull final PPrimitiveCoercionBiFunction primitiveCoercionBiFunctionProto) {
            return new PrimitiveCoercionBiFunction(PhysicalOperator.fromProto(serializationContext,
                    Objects.requireNonNull(primitiveCoercionBiFunctionProto.getOperator())));
        }
    }

    /**
     * Coercion function for arrays.
     */
    @AutoService(PlanSerialization.class)
    @ProtoMessage(PArrayCoercionBiFunction.class)
    public static class ArrayCoercionBiFunction implements MessageHelpers.CoercionBiFunction {
        @Nonnull
        private final Type.Array fromArrayType;
        @Nonnull
        private final Type.Array toArrayType;

        @Nullable
        private final CoercionTrieNode elementsTrie;

        public ArrayCoercionBiFunction(@Nonnull final Type.Array fromArrayType, @Nonnull final Type.Array toArrayType,
                                       @Nullable final CoercionTrieNode elementsTrie) {
            this.fromArrayType = fromArrayType;
            this.toArrayType = toArrayType;
            this.elementsTrie = elementsTrie;
        }

        @Override
        public Object apply(final Descriptors.Descriptor targetDescriptor, final Object current) {
            return MessageHelpers.coerceArray(toArrayType, fromArrayType, targetDescriptor, elementsTrie, current);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, fromArrayType, toArrayType, elementsTrie);
        }

        @Nonnull
        @Override
        public PArrayCoercionBiFunction toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PArrayCoercionBiFunction.Builder builder = PArrayCoercionBiFunction.newBuilder()
                    .setFromArrayType(fromArrayType.toTypeProto(serializationContext))
                    .setToArrayType(toArrayType.toTypeProto(serializationContext));

            if (elementsTrie != null) {
                builder.setElementsTrie(elementsTrie.toProto(serializationContext));
            }
            return builder.build();
        }

        @Nonnull
        @Override
        public PCoercionBiFunction toCoercionBiFunctionProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCoercionBiFunction.newBuilder().setArrayCoercionBiFunction(toProto(serializationContext)).build();
        }

        @Nonnull
        public static ArrayCoercionBiFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PArrayCoercionBiFunction arrayCoercionBiFunctionProto) {
            final CoercionTrieNode elementsTrie;
            if (arrayCoercionBiFunctionProto.hasElementsTrie()) {
                elementsTrie = CoercionTrieNode.fromProto(serializationContext, arrayCoercionBiFunctionProto.getElementsTrie());
            } else {
                elementsTrie = null;
            }
            return new ArrayCoercionBiFunction((Type.Array)Type.fromTypeProto(serializationContext, Objects.requireNonNull(arrayCoercionBiFunctionProto.getFromArrayType())),
                    (Type.Array)Type.fromTypeProto(serializationContext, Objects.requireNonNull(arrayCoercionBiFunctionProto.getFromArrayType())),
                    elementsTrie);
        }
    }
}
