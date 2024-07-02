/*
 * RecordConstructorValue.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordConstructorValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A {@link Value} that encapsulates its children {@link Value}s into a single {@link Value} of a
 * {@link Type.Record} type.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordConstructorValue extends AbstractValue implements AggregateValue, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Constructor-Value");
    @Nonnull
    private final Type.Record resultType;
    @Nonnull
    protected final List<Column<? extends Value>> columns;
    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    private RecordConstructorValue(@Nonnull Collection<Column<? extends Value>> columns, @Nonnull final Type.Record resultType) {
        this.resultType = resultType;
        this.columns = ImmutableList.copyOf(columns);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
    }

    @Nonnull
    public List<Column<? extends Value>> getColumns() {
        return columns;
    }

    @Nonnull
    @Override
    protected List<? extends Value> computeChildren() {
        return columns
                .stream()
                .map(Column::getValue)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var typeRepository = context.getTypeRepository();
        final var resultMessageBuilder = newMessageBuilderForType(typeRepository);
        final var descriptorForType = resultMessageBuilder.getDescriptorForType();
        final var fieldDescriptors = descriptorForType.getFields();

        final var fields = Objects.requireNonNull(getResultType().getFields());
        var i = 0;
        for (final var child : getChildren()) {
            final var field = fields.get(i);
            final var fieldType = field.getFieldType();
            var childResult = deepCopyIfNeeded(typeRepository, fieldType, child.eval(store, context));
            if (childResult != null) {
                final var fieldDescriptor = fieldDescriptors.get(i);
                if (fieldType.isArray() && fieldType.isNullable()) {
                    final var wrappedDescriptor = fieldDescriptor.getMessageType();
                    final var wrapperBuilder = DynamicMessage.newBuilder(wrappedDescriptor);
                    wrapperBuilder.setField(wrappedDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()), childResult);
                    childResult = wrapperBuilder.build();
                }
                resultMessageBuilder.setField(fieldDescriptor, childResult);
            } else {
                Verify.verify(fieldType.isNullable());
            }
            i++;
        }
        return resultMessageBuilder.build();
    }

    @Nonnull
    private DynamicMessage.Builder newMessageBuilderForType(@Nonnull TypeRepository typeRepository) {
        return Objects.requireNonNull(typeRepository.newMessageBuilder(getResultType()));
    }

    /**
     * Given the possible mix of messages originating from precompiled classes as well as messages that are instances of
     * {@link DynamicMessage}, we need to make sure that we actually create a cohesive object structure when one message
     * is integrated into the other when a new record is formed. All dynamic messages only ever depend on other dynamic
     * messages that make up their fields, etc. That means that in the worst case we now lazily create a dynamic message
     * from a regular message. Note that both messages are required (by their descriptors) to be wire-compatible.
     * Note that we try to avoid making a copy if at all possible.
     * TODO When this <a href="https://github.com/FoundationDB/fdb-record-layer/issues/1910">issue</a> gets addressed
     *      this code-path will become obsolete and can be removed. In fact, leaving it in wouldn't hurt as a deep copy
     *      would be deemed unnecessary.
     * @param typeRepository the type repository
     * @param fieldType the type of the field
     * @param field the object that may or may not be copied
     * @return an object that is either {@code field} if a copy could be avoided or a new copy of {@code field} whose
     *         constituent messages are {@link DynamicMessage}s based on dynamically-created descriptors.
     */
    @VisibleForTesting
    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static Object deepCopyIfNeeded(@Nonnull TypeRepository typeRepository,
                                          @Nonnull final Type fieldType,
                                          @Nullable final Object field) {
        if (field == null) {
            return null;
        }

        if (fieldType.isPrimitive()) {
            return protoObjectForPrimitive(fieldType, field);
        }

        if (fieldType instanceof Type.Array) {
            final var objects = (List<?>)field;
            final var elementType = Verify.verifyNotNull(((Type.Array)fieldType).getElementType());
            if (elementType.isPrimitive()) {
                if (elementType.getTypeCode() == Type.TypeCode.BYTES || elementType.getTypeCode() == Type.TypeCode.VERSION) {
                    var resultBuilder = ImmutableList.builderWithExpectedSize(objects.size());
                    for (Object object : objects) {
                        resultBuilder.add(protoObjectForPrimitive(elementType, object));
                    }
                    return resultBuilder.build();
                } else {
                    return field;
                }
            }
            final var resultBuilder = ImmutableList.builder();
            for (final var object : objects) {
                resultBuilder.add(Verify.verifyNotNull(deepCopyIfNeeded(typeRepository, elementType, object)));
            }
            return resultBuilder.build();
        }

        if (fieldType instanceof Type.Enum) {
            final var typeName = typeRepository.getProtoTypeName(fieldType);
            return typeRepository.getEnumValue(typeName, ((Descriptors.EnumValueDescriptor)field).getName());
        }

        //TODO if we encounter an AnyRecord we cannot ever correctly deal with this
        Verify.verify(fieldType instanceof Type.Record);
        final var message = (Message)field;
        final var declaredDescriptor = Verify.verifyNotNull(typeRepository.getMessageDescriptor(fieldType));
        return MessageHelpers.deepCopyMessageIfNeeded(declaredDescriptor, message);
    }

    private static Object protoObjectForPrimitive(@Nonnull Type type, @Nonnull Object field) {
        if (type.getTypeCode() == Type.TypeCode.BYTES) {
            if (field instanceof byte[]) {
                // todo: we're a little inconsistent about whether the field should be byte[] or ByteString for BYTES fields
                return ZeroCopyByteString.wrap((byte[]) field);
            }
        } else if (type.getTypeCode() == Type.TypeCode.VERSION) {
            return ZeroCopyByteString.wrap(((FDBRecordVersion)field).toBytes(false));
        }
        return field;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return hashCodeWithoutChildrenSupplier.get();
    }

    private int computeHashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION,
                BASE_HASH,
                columns.stream()
                        .map(column -> column.getField().hashCode())
                        .collect(ImmutableList.toImmutableList()));
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, columns);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "(" +
               columns.stream()
                       .map(column -> {
                           final var field = column.getField();
                           final var value = column.getValue();
                           if (field.getFieldNameOptional().isPresent()) {
                               return column.getValue().explain(formatter) + " as " + field.getFieldName();
                           }
                           return value.explain(formatter);
                       })
                       .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String toString() {
        return "(" +
               columns.stream()
                       .map(column -> {
                           final var field = column.getField();
                           final var value = column.getValue();
                           if (field.getFieldNameOptional().isPresent()) {
                               return column.getValue() + " as " + field.getFieldName();
                           }
                           return value.toString();
                       })
                       .collect(Collectors.joining(", ")) + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public RecordConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(columns.size() == Iterables.size(newChildren));
        //noinspection UnstableApiUsage
        final ImmutableList<Column<? extends Value>> newColumns =
                Streams.zip(StreamSupport.stream(newChildren.spliterator(), false),
                                this.columns.stream(),
                                (newChild, column) -> Column.of(column.getField(), newChild))
                        .collect(ImmutableList.toImmutableList());
        return new RecordConstructorValue(newColumns, resultType);
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final List<Object> listOfPartials = Lists.newArrayList();
        for (final var child : getChildren()) {
            Verify.verify(child instanceof AggregateValue);
            final Object childResultElement = ((AggregateValue)child).evalToPartial(store, context);
            listOfPartials.add(childResultElement);
        }
        return Collections.unmodifiableList(listOfPartials);
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(final @Nonnull TypeRepository typeRepository) {
        return new Accumulator() {

            @Nonnull
            private final List<Accumulator> childAccumulators = buildAccumulators();

            @Override
            public void accumulate(@Nullable final Object currentObject) {
                if (currentObject == null) {
                    childAccumulators.forEach(childAccumulator -> childAccumulator.accumulate(null));
                } else {
                    Verify.verify(currentObject instanceof Collection);
                    final var currentObjectAsList = (List<?>)currentObject;
                    var i = 0;
                    for (final var o : currentObjectAsList) {
                        childAccumulators.get(i).accumulate(o);
                        i++;
                    }
                }
            }

            @Nonnull
            @Override
            public Object finish() {
                final var resultMessageBuilder = newMessageBuilderForType(typeRepository);
                final var descriptorForType = resultMessageBuilder.getDescriptorForType();

                var i = 0;
                final var fields = Objects.requireNonNull(getResultType().getFields());

                for (final var childAccumulator : childAccumulators) {
                    final var finalResult = childAccumulator.finish();
                    if (finalResult != null) {
                        resultMessageBuilder.setField(descriptorForType.findFieldByNumber(fields.get(i).getFieldIndex()), finalResult);
                    }
                    i ++;
                }

                return resultMessageBuilder.build();
            }

            @Nonnull
            private List<Accumulator> buildAccumulators() {
                final ImmutableList.Builder<Accumulator> childAccumulatorsBuilder = ImmutableList.builder();
                for (final var child : getChildren()) {
                    Verify.verify(child instanceof AggregateValue);
                    childAccumulatorsBuilder.add(((AggregateValue)child).createAccumulator(typeRepository));
                }
                return childAccumulatorsBuilder.build();
            }
        };
    }

    @Nonnull
    @Override
    public PRecordConstructorValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        PRecordConstructorValue.Builder builder = PRecordConstructorValue.newBuilder();
        for (final Column<? extends Value> column : columns) {
            builder.addColumns(column.toProto(serializationContext));
        }
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setRecordConstructorValue(specificValueProto).build();
    }

    @Nonnull
    public static RecordConstructorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordConstructorValue recordConstructorValueProto) {
        final ImmutableList.Builder<Column<? extends Value>> columnsBuilder = ImmutableList.builder();
        for (int i = 0; i < recordConstructorValueProto.getColumnsCount(); i ++) {
            final PRecordConstructorValue.PColumn columnProto = recordConstructorValueProto.getColumns(i);
            columnsBuilder.add(Column.fromProto(serializationContext, columnProto));
        }
        final ImmutableList<Column<? extends Value>> columns = columnsBuilder.build();
        return new RecordConstructorValue(columns,
                (Type.Record)Type.fromTypeProto(serializationContext, Objects.requireNonNull(recordConstructorValueProto.getResultType())));
    }

    @Nonnull
    private static Type.Record computeResultType(@Nonnull final Collection<Column<? extends Value>> columns) {
        final var fields = columns.stream()
                .map(Column::getField)
                .collect(ImmutableList.toImmutableList());
        return Type.Record.fromFields(false, fields);
    }

    @Nonnull
    private static List<Column<? extends Value>> resolveColumns(@Nonnull final Type.Record recordType,
                                                                @Nonnull final Collection<Column<? extends Value>> columns) {
        final var fields = recordType.getFields();
        Verify.verify(fields.size() == columns.size());

        final var resolvedColumnsBuilder = ImmutableList.<Column<? extends Value>>builder();
        final var columnsIterator = columns.iterator();

        for (final var field : fields) {
            final var column = columnsIterator.next();
            resolvedColumnsBuilder.add(Column.of(field, column.getValue()));
        }
        return resolvedColumnsBuilder.build();
    }

    @Nonnull
    public static RecordConstructorValue ofColumns(@Nonnull final Collection<Column<? extends Value>> columns) {
        final Type.Record resolvedResultType = computeResultType(columns);
        return new RecordConstructorValue(resolveColumns(resolvedResultType, columns), resolvedResultType);
    }

    @Nonnull
    public static RecordConstructorValue ofColumnsAndName(@Nonnull final Collection<Column<? extends Value>> columns, @Nonnull final String name) {
        final Type.Record resolvedResultType = computeResultType(columns).withName(name);
        return new RecordConstructorValue(resolveColumns(resolvedResultType, columns), resolvedResultType);
    }

    @Nonnull
    public static RecordConstructorValue ofUnnamed(@Nonnull final Collection<? extends Value> arguments) {
        final List<Column<? extends Value>> columns =
                arguments.stream()
                        .map(Column::unnamedOf)
                        .collect(ImmutableList.toImmutableList());
        return ofColumns(columns);
    }

    /**
     * The {@code record} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class RecordFn extends BuiltInFunction<Value> {
        public RecordFn() {
            super("record",
                    ImmutableList.of(), new Type.Any(), (builtInFunction, arguments) -> encapsulateInternal(arguments));
        }

        @Nonnull
        private static Value encapsulateInternal(@Nonnull final List<? extends Typed> arguments) {
            final List<Column<? extends Value>> namedArguments =
                    arguments.stream()
                            .map(typed -> (Value)typed)
                            .map(Column::unnamedOf)
                            .collect(ImmutableList.toImmutableList());
            return ofColumns(namedArguments);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordConstructorValue, RecordConstructorValue> {
        @Nonnull
        @Override
        public Class<PRecordConstructorValue> getProtoMessageClass() {
            return PRecordConstructorValue.class;
        }

        @Nonnull
        @Override
        public RecordConstructorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordConstructorValue recordConstructorValueProto) {
            return RecordConstructorValue.fromProto(serializationContext, recordConstructorValueProto);
        }
    }
}
