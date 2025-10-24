/*
 * CastValue.java
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
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PCastValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A value that casts an object of a type to an object of another type using SQL CAST semantics.
 * Unlike PromoteValue which handles implicit promotions, CastValue handles explicit type conversions
 * <i>mostly</i> according to SQL standard CAST rules.
 */
@API(API.Status.EXPERIMENTAL)
public class CastValue extends AbstractValue implements ValueWithChild, Value.RangeMatchableValue {

    /**
     * Map from (sourceType, targetType) to the physical cast operator.
     */
    @Nonnull
    private static final Supplier<Map<Type.TypeCode, Map<Type.TypeCode, PhysicalOperator>>> castOperatorMapSupplier =
            Suppliers.memoize(() -> {
                final Map<Type.TypeCode, ImmutableMap.Builder<Type.TypeCode, PhysicalOperator>> builderMap = new HashMap<>();

                // Group operators by source type
                for (final PhysicalOperator physicalOperator : PhysicalOperator.values()) {
                    builderMap.computeIfAbsent(physicalOperator.getFrom(),
                            ignored -> ImmutableMap.builder())
                            .put(physicalOperator.getTo(), physicalOperator);
                }

                // Build the final nested map
                final ImmutableMap.Builder<Type.TypeCode, Map<Type.TypeCode, PhysicalOperator>> finalBuilder = ImmutableMap.builder();
                for (final Map.Entry<Type.TypeCode, ImmutableMap.Builder<Type.TypeCode, PhysicalOperator>> entry : builderMap.entrySet()) {
                    finalBuilder.put(entry.getKey(), entry.getValue().build());
                }
                return finalBuilder.build();
            });

    @Nonnull
    private final Value child;
    @Nonnull
    private final Type castToType;
    @Nonnull
    private final PhysicalOperator physicalOperator;

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Cast-Value");

    @Nonnull
    @Override
    public PCastValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PCastValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setCastToType(castToType.toTypeProto(serializationContext))
                .setOperator(physicalOperator.toProto(serializationContext))
                .build();
    }

    /**
     * This cast map is defined based on SQL standard CAST rules for explicit type conversions.
     * SQL CAST is more permissive than promotion and allows conversions like string to number,
     * number to string, etc., with appropriate error handling for invalid conversions
     */
    enum PhysicalOperator {
        // Numeric to numeric conversions (similar to promotions but explicit)
        INT_TO_LONG(Type.TypeCode.INT, Type.TypeCode.LONG, (descriptor, in) -> Long.valueOf((Integer)in)),
        INT_TO_FLOAT(Type.TypeCode.INT, Type.TypeCode.FLOAT, (descriptor, in) -> Float.valueOf((Integer)in)),
        INT_TO_DOUBLE(Type.TypeCode.INT, Type.TypeCode.DOUBLE, (descriptor, in) -> Double.valueOf((Integer)in)),
        LONG_TO_INT(Type.TypeCode.LONG, Type.TypeCode.INT, (descriptor, in) -> {
            Long value = (Long) in;
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Value out of range for INT: " + value);
            }
            return value.intValue();
        }),
        LONG_TO_FLOAT(Type.TypeCode.LONG, Type.TypeCode.FLOAT, (descriptor, in) -> Float.valueOf((Long)in)),
        LONG_TO_DOUBLE(Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (descriptor, in) -> Double.valueOf((Long)in)),
        FLOAT_TO_INT(Type.TypeCode.FLOAT, Type.TypeCode.INT, (descriptor, in) -> {
            Float value = (Float) in;
            if (value.isNaN() || value.isInfinite()) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast NaN or Infinite to INT");
            }
            return Math.round(value);
        }),
        FLOAT_TO_LONG(Type.TypeCode.FLOAT, Type.TypeCode.LONG, (descriptor, in) -> {
            Float value = (Float) in;
            if (value.isNaN() || value.isInfinite()) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast NaN or Infinite to LONG");
            }
            return Math.round(value);
        }),
        FLOAT_TO_DOUBLE(Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (descriptor, in) -> Double.valueOf((Float)in)),
        DOUBLE_TO_INT(Type.TypeCode.DOUBLE, Type.TypeCode.INT, (descriptor, in) -> {
            Double value = (Double) in;
            if (value.isNaN() || value.isInfinite()) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast NaN or Infinite to INT");
            }
            return (int) Math.round(value);
        }),
        DOUBLE_TO_LONG(Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (descriptor, in) -> {
            Double value = (Double) in;
            if (value.isNaN() || value.isInfinite()) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast NaN or Infinite to LONG");
            }
            return Math.round(value);
        }),
        DOUBLE_TO_FLOAT(Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (descriptor, in) -> {
            Double value = (Double) in;
            if (value.isNaN() || value.isInfinite()) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast NaN or Infinite to FLOAT");
            }
            if (value > Float.MAX_VALUE || value < -Float.MAX_VALUE) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Value out of range for FLOAT: " + value);
            }
            return value.floatValue();
        }),

        // Numeric to string conversions
        INT_TO_STRING(Type.TypeCode.INT, Type.TypeCode.STRING, (descriptor, in) -> in.toString()),
        LONG_TO_STRING(Type.TypeCode.LONG, Type.TypeCode.STRING, (descriptor, in) -> in.toString()),
        FLOAT_TO_STRING(Type.TypeCode.FLOAT, Type.TypeCode.STRING, (descriptor, in) -> in.toString()),
        DOUBLE_TO_STRING(Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (descriptor, in) -> in.toString()),
        BOOLEAN_TO_STRING(Type.TypeCode.BOOLEAN, Type.TypeCode.STRING, (descriptor, in) -> in.toString()),

        // String to numeric conversions (with validation)
        STRING_TO_INT(Type.TypeCode.STRING, Type.TypeCode.INT, (descriptor, in) -> {
            try {
                return Integer.parseInt(((String) in).trim());
            } catch (NumberFormatException e) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast string '" + in + "' to INT: " + e.getMessage());
                return null; // Never reached
            }
        }),
        STRING_TO_LONG(Type.TypeCode.STRING, Type.TypeCode.LONG, (descriptor, in) -> {
            try {
                return Long.parseLong(((String) in).trim());
            } catch (NumberFormatException e) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast string '" + in + "' to LONG: " + e.getMessage());
                return null; // Never reached
            }
        }),
        STRING_TO_FLOAT(Type.TypeCode.STRING, Type.TypeCode.FLOAT, (descriptor, in) -> {
            try {
                return Float.parseFloat(((String) in).trim());
            } catch (NumberFormatException e) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast string '" + in + "' to FLOAT: " + e.getMessage());
                return null; // Never reached
            }
        }),
        STRING_TO_DOUBLE(Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (descriptor, in) -> {
            try {
                return Double.parseDouble(((String) in).trim());
            } catch (NumberFormatException e) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast string '" + in + "' to DOUBLE: " + e.getMessage());
                return null; // Never reached
            }
        }),

        // Boolean conversions
        STRING_TO_BOOLEAN(Type.TypeCode.STRING, Type.TypeCode.BOOLEAN, (descriptor, in) -> {
            String str = ((String) in).trim().toLowerCase(Locale.ROOT);
            if ("true".equals(str) || "1".equals(str)) {
                return Boolean.TRUE;
            } else if ("false".equals(str) || "0".equals(str)) {
                return Boolean.FALSE;
            } else {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Cannot cast string '" + in + "' to BOOLEAN");
                return null; // Never reached
            }
        }),
        INT_TO_BOOLEAN(Type.TypeCode.INT, Type.TypeCode.BOOLEAN, (descriptor, in) -> !in.equals(0)),
        BOOLEAN_TO_INT(Type.TypeCode.BOOLEAN, Type.TypeCode.INT, (descriptor, in) -> ((Boolean) in) ? 1 : 0),

        // NULL conversions (same as PromoteValue)
        NULL_TO_INT(Type.TypeCode.NULL, Type.TypeCode.INT, (descriptor, in) -> (Integer) null),
        NULL_TO_LONG(Type.TypeCode.NULL, Type.TypeCode.LONG, (descriptor, in) -> (Long) null),
        NULL_TO_FLOAT(Type.TypeCode.NULL, Type.TypeCode.FLOAT, (descriptor, in) -> (Float) null),
        NULL_TO_DOUBLE(Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (descriptor, in) -> (Double) null),
        NULL_TO_BOOLEAN(Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (descriptor, in) -> (Boolean) null),
        NULL_TO_STRING(Type.TypeCode.NULL, Type.TypeCode.STRING, (descriptor, in) -> (String) null),
        NULL_TO_ARRAY(Type.TypeCode.NULL, Type.TypeCode.ARRAY, (descriptor, in) -> null),
        NULL_TO_RECORD(Type.TypeCode.NULL, Type.TypeCode.RECORD, (descriptor, in) -> null),
        NULL_TO_ENUM(Type.TypeCode.NULL, Type.TypeCode.ENUM, (descriptor, in) -> null),
        NULL_TO_UUID(Type.TypeCode.NULL, Type.TypeCode.UUID, (descriptor, in) -> null),

        ARRAY_TO_ARRAY(Type.TypeCode.ARRAY, Type.TypeCode.ARRAY, CastValue::castArrayToArray),

        // STRING to complex types.
        STRING_TO_ENUM(Type.TypeCode.STRING, Type.TypeCode.ENUM, ((descriptor, in) -> PromoteValue.PhysicalOperator.stringToEnumValue((Descriptors.EnumDescriptor)descriptor, (String)in))),
        STRING_TO_UUID(Type.TypeCode.STRING, Type.TypeCode.UUID, ((descriptor, in) -> PromoteValue.PhysicalOperator.stringToUuidValue((String) in))),

        ARRAY_TO_VECTOR(Type.TypeCode.ARRAY, Type.TypeCode.VECTOR, CastValue::castArrayToVector);

        @Nonnull
        private final Type.TypeCode from;
        @Nonnull
        private final Type.TypeCode to;
        @Nonnull
        private final BiFunction<Descriptors.GenericDescriptor, Object, Object> castFunction;

        @Nonnull
        private static final Supplier<BiMap<PhysicalOperator, PCastValue.PPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(PhysicalOperator.class,
                        PCastValue.PPhysicalOperator.class));

        PhysicalOperator(@Nonnull final Type.TypeCode from,
                        @Nonnull final Type.TypeCode to,
                        @Nonnull final BiFunction<Descriptors.GenericDescriptor, Object, Object> castFunction) {
            this.from = from;
            this.to = to;
            this.castFunction = castFunction;
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
        public BiFunction<Descriptors.GenericDescriptor, Object, Object> getCastFunction() {
            return castFunction;
        }

        @Nonnull
        public PCastValue.PPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @Nonnull
        public static PhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PCastValue.PPhysicalOperator physicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(physicalOperatorProto));
        }

        @Nonnull
        private static BiMap<PhysicalOperator, PCastValue.PPhysicalOperator> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }
    }

    private CastValue(@Nonnull final Value child,
                     @Nonnull final Type castToType,
                     @Nonnull final PhysicalOperator physicalOperator) {
        this.child = child;
        this.castToType = castToType;
        this.physicalOperator = physicalOperator;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return castToType;
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var childTokens = Iterables.getOnlyElement(explainSuppliers).get();
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("CAST",
                childTokens.getExplainTokens().addWhitespace().addKeyword("AS").addWhitespace()
                        .addNested(castToType.describe())));
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new CastValue(rebasedChild, castToType, physicalOperator);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final var childResult = getChild().eval(store, context);
        if (childResult == null) {
            return null;
        }

        // Special handling for array to other types.
        if (physicalOperator.equals(PhysicalOperator.ARRAY_TO_ARRAY)) {
            final Type inType = child.getResultType();
            final var fromArray = (Type.Array) inType;
            final var toArray = (Type.Array) castToType;
            final var descriptor = new ArrayCastDescriptor(fromArray, toArray);
            return castArrayToArray(descriptor, childResult);
        } else if (physicalOperator.equals(PhysicalOperator.ARRAY_TO_VECTOR)) {
            final Type inType = child.getResultType();
            final var fromArray = (Type.Array) inType;
            final var descriptor = new ArrayCastDescriptor(fromArray, castToType);
            return castArrayToVector(descriptor, childResult);
        }

        return physicalOperator.getCastFunction().apply(null, childResult);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(castToType, physicalOperator);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> {
                    final CastValue that = (CastValue)other;
                    return that.castToType.equals(this.castToType);
                });
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, child, castToType, physicalOperator);
    }

    @Override
    public String toString() {
        return "CAST(" + child + " AS " + castToType + ")";
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCastValue(toProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    /**
     * Creates a CastValue that casts the input value to the target type.
     *
     * @param inValue the value to cast
     * @param castToType the target type to cast to
     * @return a CastValue that performs the cast, or the original value if no cast is needed
     * @throws SemanticException if the cast is not supported
     */
    @Nonnull
    public static Value inject(@Nonnull final Value inValue, @Nonnull final Type castToType) {
        final Type inType = inValue.getResultType();

        // If types are the same, no cast needed
        if (inType.equals(castToType)) {
            return inValue;
        }

        final Type.TypeCode fromTypeCode = inType.getTypeCode();
        final Type.TypeCode toTypeCode = castToType.getTypeCode();

        // Special handling of Collections' bottom type.
        if (fromTypeCode == Type.TypeCode.NONE) {
            if (!castToType.isArray()) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST,
                        "Cannot cast empty array to non-array type");
            }
            // immediately return an empty array with the designated target type.
            final var elementType = Verify.verifyNotNull(((Type.Array)castToType).getElementType());
            return AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray(elementType);
        }

        // Special handling for array to array casts
        if (fromTypeCode == Type.TypeCode.ARRAY && toTypeCode == Type.TypeCode.ARRAY) {
            final var fromArray = (Type.Array) inType;
            final var toArray = (Type.Array) castToType;

            // Check if the cast is supported
            if (!isCastSupported(inType, castToType)) {
                SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST,
                        "Cannot cast array with element type " + fromArray.getElementType() +
                        " to array with element type " + toArray.getElementType());
            }
            return new CastValue(inValue, castToType, PhysicalOperator.ARRAY_TO_ARRAY);
        }

        // Look up the cast operator for non-array types
        final Map<Type.TypeCode, Map<Type.TypeCode, PhysicalOperator>> castOperatorMap = castOperatorMapSupplier.get();
        final Map<Type.TypeCode, PhysicalOperator> fromMap = castOperatorMap.get(fromTypeCode);

        if (fromMap == null) {
            SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "No cast defined from " + fromTypeCode + " to " + toTypeCode);
        }

        final PhysicalOperator physicalOperator = fromMap.get(toTypeCode);
        if (physicalOperator == null) {
            SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "No cast defined from " + fromTypeCode + " to " + toTypeCode);
        }

        return new CastValue(inValue, castToType, physicalOperator);
    }

    /**
     * Checks if a cast is supported from the source type to the target type.
     * For arrays, recursively checks if the element types can be cast.
     *
     * @param fromType the source type
     * @param toType the target type
     * @return true if the cast is supported, false otherwise
     */
    public static boolean isCastSupported(@Nonnull final Type fromType, @Nonnull final Type toType) {
        if (fromType.equals(toType)) {
            return true;
        }

        // Special handling for arrays - check element type compatibility recursively
        if (fromType.isArray() && toType.isArray()) {
            final var fromArray = (Type.Array) fromType;
            final var toArray = (Type.Array) toType;
            final var fromElementType = fromArray.getElementType();
            final var toElementType = toArray.getElementType();

            // Handle empty arrays (null element types) - they can be cast to any array type
            if (fromElementType == null || toElementType == null) {
                return true;
            }

            // Recursively check if element types can be cast
            return isCastSupported(fromElementType, toElementType);
        }

        final Map<Type.TypeCode, Map<Type.TypeCode, PhysicalOperator>> castOperatorMap = castOperatorMapSupplier.get();
        final Map<Type.TypeCode, PhysicalOperator> fromMap = castOperatorMap.get(fromType.getTypeCode());

        return fromMap != null && fromMap.containsKey(toType.getTypeCode());
    }

    @Nonnull
    public static CastValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PCastValue castValueProto) {
        final Value child = Value.fromValueProto(serializationContext, castValueProto.getChild());
        final Type castToType = Type.fromTypeProto(serializationContext, castValueProto.getCastToType());
        final PhysicalOperator physicalOperator = PhysicalOperator.fromProto(serializationContext, castValueProto.getOperator());
        return new CastValue(child, castToType, physicalOperator);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCastValue, CastValue> {
        @Nonnull
        @Override
        public Class<PCastValue> getProtoMessageClass() {
            return PCastValue.class;
        }

        @Nonnull
        @Override
        public CastValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PCastValue castValueProto) {
            return CastValue.fromProto(serializationContext, castValueProto);
        }
    }

    /**
     * Performs array to array casting by recursively casting each element.
     * Handles empty arrays by creating an empty array of the target type.
     *
     * @param descriptor the type descriptor (contains source and target array types)
     * @param in the input array to cast
     * @return the cast array
     */
    @Nullable
    @SuppressWarnings("unchecked")
    private static Object castArrayToArray(Object descriptor, Object in) {
        if (in == null) {
            return null;
        }

        final var typeDescriptor = (ArrayCastDescriptor) descriptor;
        final var sourceArrayType = typeDescriptor.getFromType();
        final var targetArrayType = typeDescriptor.getToType();
        final var sourceElementType = sourceArrayType.getElementType();
        Verify.verify(targetArrayType.isArray());
        final var targetElementType = ((Type.Array)targetArrayType).getElementType();

        // Check for null element types
        if (targetElementType == null) {
            SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Target array element type cannot be null");
        }

        final var inputList = (java.util.List<Object>) in;

        // Handle empty arrays - return empty list of target type
        if (inputList.isEmpty()) {
            return new java.util.ArrayList<>();
        }

        // Cast each element recursively
        final var resultList = new java.util.ArrayList<>();
        for (final Object element : inputList) {
            if (element == null) {
                resultList.add(null);
            } else if (sourceElementType != null && sourceElementType.equals(targetElementType)) {
                // No casting needed for elements
                resultList.add(element);
            } else {
                // Recursively cast elements
                if (sourceElementType == null) {
                    SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Source array element type cannot be null");
                }
                final var elementCastValue = inject(new LiteralValue<>(sourceElementType, element), targetElementType);
                final var evalContext = EvaluationContext.empty();
                // child values are evaluated already, therefore, we do not need the EvaluationContext here
                final var castElement = elementCastValue.evalWithoutStore(evalContext);
                resultList.add(castElement);
            }
        }

        return resultList;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static Object castArrayToVector(Object descriptor, Object in) {
        if (in == null) {
            return null;
        }

        final var typeDescriptor = (ArrayCastDescriptor) descriptor;
        final var sourceArrayType = typeDescriptor.getFromType();
        final var targetArrayType = typeDescriptor.getToType();
        final var sourceElementType = sourceArrayType.getElementType();
        Verify.verify(targetArrayType.isVector());
        final var targetVectorType = (Type.Vector)targetArrayType;

        final var inputList = (java.util.List<Object>) in;

        if (sourceElementType == null) {
            SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Source array element type cannot be null");
        }

        if (inputList.size() != targetVectorType.getDimensions()) {
            SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "Source array is not the same size of the vector");
        }

        return parseVector(inputList, targetVectorType, sourceElementType);
    }

    /**
     * Descriptor for array to array casting operations.
     */
    private static final class ArrayCastDescriptor {
        private final Type.Array fromType;
        private final Type toType;

        public ArrayCastDescriptor(Type.Array fromType, Type toType) {
            this.fromType = fromType;
            this.toType = toType;
        }

        public Type.Array getFromType() {
            return fromType;
        }

        public Type getToType() {
            return toType;
        }
    }

    @Nonnull
    private static RealVector parseHalfVector(@Nonnull final List<Object> array, @Nonnull final Type sourceElementType) {
        final var sourceTypeCode = sourceElementType.getTypeCode();
        if (sourceTypeCode == Type.TypeCode.FLOAT) {
            final var halfArray = array.stream().map(obj -> Half.valueOf((Float)obj)).toArray(Half[]::new);
            return new HalfRealVector(halfArray);
        }
        if (sourceTypeCode == Type.TypeCode.DOUBLE) {


            final var doubleArray = array.stream().map(obj -> ((Number)obj).doubleValue()).mapToDouble(Double::doubleValue).toArray();
            return new HalfRealVector(doubleArray);
        }
        if (sourceTypeCode == Type.TypeCode.INT) {
            final var intArray = array.stream().map(obj -> ((Number)obj).intValue()).mapToInt(Integer::intValue).toArray();
            return new HalfRealVector(intArray);
        }
        if (sourceTypeCode == Type.TypeCode.LONG) {
            final var longArray = array.stream().map(obj -> ((Number)obj).longValue()).mapToLong(Long::longValue).toArray();
            return new HalfRealVector(longArray);
        }
        SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "can not cast array of " + sourceElementType + " to half vector");
        return null; // not reachable.
    }

    @Nonnull
    private static RealVector parseFloatVector(@Nonnull final List<Object> array, @Nonnull final Type sourceElementType) {
        final var sourceTypeCode = sourceElementType.getTypeCode();
        if (sourceTypeCode == Type.TypeCode.FLOAT) {
            final var floatArray = toFloatArray(array.stream().map(obj -> ((Number)obj).floatValue()));
            return new FloatRealVector(floatArray);
        }
        if (sourceTypeCode == Type.TypeCode.DOUBLE) {
            final var doubleArray = array.stream().map(obj -> ((Number)obj).doubleValue()).mapToDouble(Double::doubleValue).toArray();
            return new FloatRealVector(doubleArray);
        }
        if (sourceTypeCode == Type.TypeCode.INT) {
            final var intArray = array.stream().map(obj -> ((Number)obj).intValue()).mapToInt(Integer::intValue).toArray();
            return new FloatRealVector(intArray);
        }
        if (sourceTypeCode == Type.TypeCode.LONG) {
            final var longArray = array.stream().map(obj -> ((Number)obj).longValue()).mapToLong(Long::longValue).toArray();
            return new FloatRealVector(longArray);
        }
        SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "can not cast array of " + sourceElementType + " to float vector");
        return null; // not reachable.
    }

    @Nonnull
    private static RealVector parseDoubleVector(@Nonnull final List<Object> array, @Nonnull final Type sourceElementType) {
        final var sourceTypeCode = sourceElementType.getTypeCode();
        if (sourceTypeCode == Type.TypeCode.DOUBLE) {
            final var doubleArray = array.stream().map(obj -> ((Number)obj).doubleValue()).toArray(Double[]::new);
            return new DoubleRealVector(doubleArray);
        }
        if (sourceTypeCode == Type.TypeCode.INT) {
            final var intArray = array.stream().map(obj -> ((Number)obj).intValue()).mapToInt(Integer::intValue).toArray();
            return new DoubleRealVector(intArray);
        }
        if (sourceTypeCode == Type.TypeCode.LONG) {
            final var longArray = array.stream().map(obj -> ((Number)obj).longValue()).mapToLong(Long::longValue).toArray();
            return new DoubleRealVector(longArray);
        }
        SemanticException.fail(SemanticException.ErrorCode.INVALID_CAST, "can not cast array of " + sourceElementType + " to double vector");
        return null; // not reachable.
    }


    @Nonnull
    private static RealVector parseVector(@Nonnull final List<Object> array, @Nonnull final Type.Vector vectorType,
                                         @Nonnull final Type sourceElementType) {
        if (vectorType.getPrecision() == 16) {
            return parseHalfVector(array, sourceElementType);
        }
        if (vectorType.getPrecision() == 32) {
            return parseFloatVector(array, sourceElementType);
        }
        if (vectorType.getPrecision() == 64) {
            return parseDoubleVector(array, sourceElementType);
        }
        throw new RecordCoreException("unexpected vector type " + vectorType);
    }

    @Nonnull
    private static float[] toFloatArray(@Nonnull final Stream<Float> floatStream) {
        List<Float> floatList = floatStream.collect(Collectors.toList());
        float[] floatArray = new float[floatList.size()];
        for (int i = 0; i < floatList.size(); i++) {
            floatArray[i] = floatList.get(i);
        }
        return floatArray;
    }
}
