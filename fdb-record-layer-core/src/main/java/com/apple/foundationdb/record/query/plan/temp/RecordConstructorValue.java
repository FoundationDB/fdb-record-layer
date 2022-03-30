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
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
    protected final List<Column<? extends Value>> columns;
    @Nonnull
    private final Supplier<List<? extends Value>> childrenSupplier;
    @Nonnull
    private final Supplier<Type.Record> resultTypeSupplier;

    private RecordConstructorValue(@Nonnull Collection<Column<? extends Value>> columns) {
        this.columns = ImmutableList.copyOf(columns);
        this.childrenSupplier = Suppliers.memoize(this::computeChildren);
        this.resultTypeSupplier = Suppliers.memoize(this::computeResultType);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return childrenSupplier.get();
    }

    private List<? extends Value> computeChildren() {
        return columns
                .stream()
                .map(Column::getValue)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        return resultTypeSupplier.get();
    }

    @Nonnull
    public Type.Record computeResultType() {
        final var fields = columns
                .stream()
                .map(Column::getField)
                .collect(ImmutableList.toImmutableList());
        return Type.Record.fromFields(fields);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        final var resultMessageBuilder = newMessageBuilderForType(context.getTypeRepository());
        final var descriptorForType = resultMessageBuilder.getDescriptorForType();

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
    private DynamicMessage.Builder newMessageBuilderForType(@Nonnull TypeRepository typeRepository) {
        return Objects.requireNonNull(typeRepository.newMessageBuilder(getResultType()));
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, columns);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, columns);
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
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
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
        return new RecordConstructorValue(newColumns);
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

    private static Value encapsulateInternal(@Nonnull final List<Typed> arguments) {
        final ImmutableList<Column<? extends Value>> namedArguments =
                arguments.stream()
                        .map(typed -> (Value)typed)
                        .map(Column::unnamedOf)
                        .collect(ImmutableList.toImmutableList());
        return new RecordConstructorValue(namedArguments);
    }

    public static RecordConstructorValue ofColumns(@Nonnull final Collection<Column<? extends Value>> columns) {
        return new RecordConstructorValue(columns);
    }

    public static RecordConstructorValue ofUnnamed(@Nonnull final Collection<? extends Value> arguments) {
        return new RecordConstructorValue(arguments.stream()
                        .map(Column::unnamedOf)
                        .collect(ImmutableList.toImmutableList()));
    }

    @AutoService(BuiltInFunction.class)
    public static class RecordFn extends BuiltInFunction<Value> {
        public RecordFn() {
            super("record",
                    ImmutableList.of(), new Type.Any(), (parserContext, builtInFunction, arguments) -> encapsulateInternal(arguments));
        }
    }
}
