/*
 * PromoteValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.planprotos.PArrayCoercionBiFunction;
import com.apple.foundationdb.record.planprotos.PCoercionBiFunction;
import com.apple.foundationdb.record.planprotos.PPrimitiveCoercionBiFunction;
import com.apple.foundationdb.record.planprotos.PPrimitiveCoercionBiFunction.PPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PPromoteValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers.CoercionTrieNode;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A value that promotes an object of a type to an object of another type. Promotions in general agree with
 * promotions according to the SQL standard.
 */
@API(API.Status.EXPERIMENTAL)
public class PromoteValue extends AbstractValue implements ValueWithChild, Value.RangeMatchableValue {
    /**
     * This promotion map is defined based on the basic SQL promotion rules for standard SQL data types when
     * applied to our data model.
     */
    enum PhysicalOperator {
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
        NULL_TO_RECORD(Type.TypeCode.NULL, Type.TypeCode.RECORD, (descriptor, in) -> null),
        NONE_TO_ARRAY(Type.TypeCode.NONE, Type.TypeCode.ARRAY, (descriptor, in) -> in),
        NULL_TO_ENUM(Type.TypeCode.NULL, Type.TypeCode.ENUM, (descriptor, in) -> null),
        STRING_TO_ENUM(Type.TypeCode.STRING, Type.TypeCode.ENUM, ((descriptor, in) -> ((Descriptors.EnumDescriptor)descriptor).findValueByName((String)in)));

        @Nonnull
        private static final Supplier<BiMap<PhysicalOperator, PPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(PhysicalOperator.class, PPhysicalOperator.class));

        @Nonnull
        private final Type.TypeCode from;
        @Nonnull
        private final Type.TypeCode to;
        @Nonnull
        private final BiFunction<Descriptors.GenericDescriptor, Object, Object> promotionFunction;

        PhysicalOperator(@Nonnull final Type.TypeCode from, @Nonnull final Type.TypeCode to,
                         @Nonnull final BiFunction<Descriptors.GenericDescriptor, Object, Object> promotionFunction) {
            this.from = from;
            this.to = to;
            this.promotionFunction = promotionFunction;
        }

        @Nonnull
        public Type.TypeCode getFrom() {
            return from;
        }

        @Nonnull
        public Type.TypeCode getTo() {
            return to;
        }

        @Nonnull
        public BiFunction<Descriptors.GenericDescriptor, Object, Object> getPromotionFunction() {
            return promotionFunction;
        }

        public Object apply(@Nullable final Descriptors.GenericDescriptor descriptor, @Nullable Object in) {
            return promotionFunction.apply(descriptor, in);
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static PhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull PPhysicalOperator physicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(physicalOperatorProto));
        }

        @Nonnull
        private static BiMap<PhysicalOperator, PPhysicalOperator> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }
    }

    private static final Map<NonnullPair<Type.TypeCode, Type.TypeCode>, PhysicalOperator> PROMOTION_MAP;

    static {
        final var mapBuilder = ImmutableMap.<NonnullPair<Type.TypeCode, Type.TypeCode>, PhysicalOperator>builder();
        for (final var operator : PhysicalOperator.values()) {
            mapBuilder.put(NonnullPair.of(operator.getFrom(), operator.getTo()), operator);
        }
        PROMOTION_MAP = mapBuilder.build();
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

        final Descriptors.GenericDescriptor genericDescriptor;
        if (isSimplePromotion) {
            genericDescriptor = null;
        } else {
            if (promoteToType.isEnum()) {
                genericDescriptor = context.getTypeRepository().getEnumDescriptor(promoteToType);
            } else {
                Verify.verify(promoteToType.isRecord());
                genericDescriptor = context.getTypeRepository().getMessageDescriptor(promoteToType);
            }
        }
        return MessageHelpers.coerceObject(promotionTrie,
                promoteToType,
                genericDescriptor,
                inValue.getResultType(),
                result);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return promoteToType;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
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
        return semanticEquals(other, AliasMap.emptyMap());
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
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setPromoteValue(toProto(serializationContext)).build();
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
        if (targetType.getTypeCode() == Type.TypeCode.ANY) {
            return null;
        }

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

        if (currentType.isEnum()) {
            SemanticException.check(currentType.withNullability(false).equals(targetType.withNullability(false)),
                    SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return null;
        }

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
    static PhysicalOperator resolvePhysicalOperator(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        return PROMOTION_MAP.get(NonnullPair.of(inType.getTypeCode(), promoteToType.getTypeCode()));
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
        if (inType.isRecord() && promoteToType.isRecord()) {
            final List<Type> inTypeElements = Objects.requireNonNull(((Type.Record) inType).getElementTypes());
            final List<Type> promoteToTypeElements = Objects.requireNonNull(((Type.Record) promoteToType).getElementTypes());
            SemanticException.check(inTypeElements.size() == promoteToTypeElements.size(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            for (int i = 0; i < inTypeElements.size(); i++) {
                SemanticException.check(!isPromotionNeeded(inTypeElements.get(i), promoteToTypeElements.get(i)), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
            return false;
        }
        SemanticException.check(inType.isPrimitive() && promoteToType.isPrimitive() ||
                inType.isPrimitive() && promoteToType.isEnum(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return inType.getTypeCode() != promoteToType.getTypeCode();
    }

    /**
     * A coercion function for primitive types.
     */
    public static class PrimitiveCoercionBiFunction implements MessageHelpers.CoercionBiFunction {
        @Nonnull
        private final PhysicalOperator operator;

        private PrimitiveCoercionBiFunction(@Nonnull final PhysicalOperator operator) {
            this.operator = operator;
        }

        @Override
        public Object apply(final Descriptors.GenericDescriptor targetDescriptor, final Object current) {
            return operator.apply(targetDescriptor, current);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operator.name());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PrimitiveCoercionBiFunction)) {
                return false;
            }
            final PrimitiveCoercionBiFunction that = (PrimitiveCoercionBiFunction)o;
            return operator == that.operator;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, operator);
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

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PPrimitiveCoercionBiFunction, PrimitiveCoercionBiFunction> {
            @Nonnull
            @Override
            public Class<PPrimitiveCoercionBiFunction> getProtoMessageClass() {
                return PPrimitiveCoercionBiFunction.class;
            }

            @Nonnull
            @Override
            public PrimitiveCoercionBiFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PPrimitiveCoercionBiFunction primitiveCoercionBiFunctionProto) {
                return PrimitiveCoercionBiFunction.fromProto(serializationContext, primitiveCoercionBiFunctionProto);
            }
        }
    }

    /**
     * Coercion function for arrays.
     */
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
        public Object apply(final Descriptors.GenericDescriptor targetDescriptor, final Object current) {
            return MessageHelpers.coerceArray(toArrayType, fromArrayType, targetDescriptor, elementsTrie, current);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fromArrayType, toArrayType, elementsTrie);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ArrayCoercionBiFunction)) {
                return false;
            }
            final ArrayCoercionBiFunction that = (ArrayCoercionBiFunction)o;
            return Objects.equals(fromArrayType, that.fromArrayType) &&
                    Objects.equals(toArrayType, that.toArrayType) &&
                    Objects.equals(elementsTrie, that.elementsTrie);
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
                    (Type.Array)Type.fromTypeProto(serializationContext, Objects.requireNonNull(arrayCoercionBiFunctionProto.getToArrayType())),
                    elementsTrie);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PArrayCoercionBiFunction, ArrayCoercionBiFunction> {
            @Nonnull
            @Override
            public Class<PArrayCoercionBiFunction> getProtoMessageClass() {
                return PArrayCoercionBiFunction.class;
            }

            @Nonnull
            @Override
            public ArrayCoercionBiFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PArrayCoercionBiFunction arrayCoercionBiFunctionProto) {
                return ArrayCoercionBiFunction.fromProto(serializationContext, arrayCoercionBiFunctionProto);
            }
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PPromoteValue, PromoteValue> {
        @Nonnull
        @Override
        public Class<PPromoteValue> getProtoMessageClass() {
            return PPromoteValue.class;
        }

        @Nonnull
        @Override
        public PromoteValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PPromoteValue promoteValueProto) {
            return PromoteValue.fromProto(serializationContext, promoteValueProto);
        }
    }
}
