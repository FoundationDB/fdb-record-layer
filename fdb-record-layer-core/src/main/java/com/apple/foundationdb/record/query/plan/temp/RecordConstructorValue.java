/*
 * RecordConstructorValue.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A {@link Value} that encapsulates its children {@link Value}s into a single {@link Value} of a
 * {@link com.apple.foundationdb.record.query.plan.temp.Type.Record} type.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordConstructorValue implements Value, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Constructor-Value");
    @Nonnull
    protected final List<Pair<? extends Value, Optional<String>>> childrenAndNames;
    @Nonnull
    private final Supplier<List<? extends Value>> childrenSupplier;
    @Nonnull
    private final Supplier<Type.Record> resultTypeSupplier;

    private RecordConstructorValue(@Nonnull List<Pair<? extends Value, Optional<String>>> childrenAndNames) {
        this.childrenAndNames = ImmutableList.copyOf(childrenAndNames);
        this.childrenSupplier = Suppliers.memoize(this::computeChildren);
        this.resultTypeSupplier = Suppliers.memoize(this::computeResultType);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return childrenSupplier.get();
    }

    private List<? extends Value> computeChildren() {
        return childrenAndNames
                .stream()
                .map(Pair::getKey)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        return resultTypeSupplier.get();
    }

    @Nonnull
    public Type.Record computeResultType() {
        final ImmutableList<Type.Record.Field> fields = childrenAndNames
                .stream()
                .map(childAndName -> Type.Record.Field.of(childAndName.getKey().getResultType(), childAndName.getValue()))
                .collect(ImmutableList.toImmutableList());
        return Type.Record.fromFields(fields);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        final DynamicMessage.Builder resultMessageBuilder = newMessageBuilderForType(context.getDynamicSchema());
        final Descriptors.Descriptor descriptorForType = resultMessageBuilder.getDescriptorForType();

        int i = 0;
        final List<Type.Record.Field> fields = Objects.requireNonNull(getResultType().getFields());

        for (final Value child : getChildren()) {
            final Object childResultElement = child.eval(store, context, record, message);
            if (childResultElement != null) {
                resultMessageBuilder.setField(descriptorForType.findFieldByNumber(fields.get(i).getFieldIndex()), childResultElement);
            }
            i++;
        }

        return resultMessageBuilder.build();
    }

    @Nonnull
    private DynamicMessage.Builder newMessageBuilderForType(@Nonnull DynamicSchema dynamicSchema) {
        return Objects.requireNonNull(dynamicSchema.newMessageBuilder(getResultType()));
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, childrenAndNames);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, childrenAndNames);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public String describe(@Nonnull final Formatter formatter) {
        return "(" +
               childrenAndNames.stream()
                       .map(childAndName -> {
                           if (childAndName.getValue().isPresent()) {
                               return childAndName.getKey().describe(formatter) + " as " + childAndName.getValue().get();
                           }
                           return childAndName.getKey().describe(formatter);
                       })
                       .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String toString() {
        return "(" +
               childrenAndNames.stream()
                       .map(childAndName -> {
                           if (childAndName.getValue().isPresent()) {
                               return childAndName.getKey() + " as " + childAndName.getValue().get();
                           }
                           return childAndName.getKey().toString();
                       })
                       .collect(Collectors.joining(", ")) + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Nonnull
    @Override
    public RecordConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(childrenAndNames.size() == Iterables.size(newChildren));
        //noinspection UnstableApiUsage
        final ImmutableList<Pair<? extends Value, Optional<String>>> newChildrenAndNames =
                Streams.zip(StreamSupport.stream(newChildren.spliterator(), false),
                        this.childrenAndNames.stream(),
                        (newChild, childAndName) -> Pair.of(newChild, childAndName.getValue()))
                        .collect(ImmutableList.toImmutableList());

        return new RecordConstructorValue(newChildrenAndNames);
    }

    public static List<Value> tryUnwrapIfTuple(@Nonnull final List<Value> values) {
        if (values.size() != 1) {
            return values;
        }

        final Value onlyElement = Iterables.getOnlyElement(values);
        if (!(onlyElement instanceof RecordConstructorValue)) {
            return values;
        }

        return ImmutableList.copyOf(onlyElement.getChildren());
    }

    public static RecordConstructorValue wrapIfNotTuple(@Nonnull final Value value) {
        if (value instanceof RecordConstructorValue) {
            return (RecordConstructorValue)value;
        }

        return RecordConstructorValue.ofUnnamed(value);
    }

    private static Value encapsulateInternal(@Nonnull final List<Typed> arguments) {
        final ImmutableList<Pair<? extends Value, Optional<String>>> namedArguments =
                arguments.stream()
                        .map(typed -> Pair.of((Value)typed, Optional.<String>empty()))
                        .map(pair -> {
                            return Pair.of(pair.getKey(), pair.getValue()); })
                        .collect(ImmutableList.toImmutableList());
        return new RecordConstructorValue(namedArguments);
    }

    public static RecordConstructorValue of(@Nonnull final List<Pair<? extends Value, Optional<String>>> namedArguments) {
        return new RecordConstructorValue(namedArguments);
    }

    public static RecordConstructorValue of(@Nonnull final Pair<? extends Value, Optional<String>> namedArgument) {
        return new RecordConstructorValue(ImmutableList.of(namedArgument));
    }

    public static RecordConstructorValue ofUnnamed(@Nonnull final Value argument) {
        return new RecordConstructorValue(ImmutableList.of(Pair.of(argument, Optional.empty())));
    }

    public static RecordConstructorValue ofUnnamed(@Nonnull final Collection<? extends Value> arguments) {
        return new RecordConstructorValue(arguments.stream()
                        .map(argument -> Pair.of(argument, Optional.<String>empty()))
                        .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static Value flattenRecords(@Nonnull final List<? extends Value> values) {
        Verify.verify(!values.isEmpty());

        final ImmutableList.Builder<Pair<? extends Value, Optional<String>>> childrenAndNamesBuilder = ImmutableList.builder();
        for (final Value value : values) {
            final Type type = value.getResultType();

            if (type.getTypeCode() != Type.TypeCode.RECORD) {
                childrenAndNamesBuilder.add(Pair.of(value, Optional.empty()));
            } else {
                final Type.Record recordType = (Type.Record)type;
                final List<? extends Value> elementValues = Type.Record.deconstructRecord(value);
                final List<Type.Record.Field> fields = Objects.requireNonNull(recordType.getFields());
                Verify.verify(elementValues.size() == fields.size());
                for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
                    final Type.Record.Field field = fields.get(i);
                    childrenAndNamesBuilder.add(Pair.of(elementValues.get(i), field.getFieldNameOptional()));
                }
            }
        }
        return RecordConstructorValue.of(childrenAndNamesBuilder.build());
    }

    @AutoService(BuiltInFunction.class)
    public static class RecordFn extends BuiltInFunction<Value> {
        public RecordFn() {
            super("record",
                    ImmutableList.of(), new Type.Any(), (parserContext, builtInFunction, arguments) -> encapsulateInternal(arguments));
        }
    }
}
