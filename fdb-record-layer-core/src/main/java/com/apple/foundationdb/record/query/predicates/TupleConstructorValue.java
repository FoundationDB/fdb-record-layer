/*
 * TupleConstructorValue.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.norse.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class TupleConstructorValue implements Value, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Tuple-Value");
    @Nonnull
    protected final List<Pair<? extends Value, Optional<String>>> childrenAndNames;
    @Nonnull
    private final Supplier<List<? extends Value>> childrenSupplier;
    @Nonnull
    private final Supplier<Type.Record> resultTypeSupplier;

    @Nonnull
    private final Cache<DynamicSchema, String> protoTypeNameCache =
            CacheBuilder.newBuilder()
                    .maximumSize(3)
                    .build();

    private TupleConstructorValue(@Nonnull List<Pair<? extends Value, Optional<String>>> childrenAndNames) {
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
        final DynamicMessage.Builder tupleResult = DynamicMessage.newBuilder(descriptorForType);

        int i = 0;
        final List<Type.Record.Field> fields = Objects.requireNonNull(getResultType().getFields());

        for (final Value child : getChildren()) {
            final Object childResultElement = child.eval(store, context, record, message);
            if (childResultElement != null) {
                tupleResult.setField(descriptorForType.findFieldByNumber(fields.get(i).getFieldIndex()), childResultElement);
            }
            i ++;
        }

        return tupleResult.build();
    }

    @Nonnull
    private DynamicMessage.Builder newMessageBuilderForType(@Nonnull DynamicSchema dynamicSchema) {
        try {
            return dynamicSchema.newMessageBuilder(protoTypeNameCache.get(dynamicSchema, () -> dynamicSchema.getProtoTypeName(getResultType())));
        } catch (final ExecutionException ee) {
            throw new RecordCoreException("unable to retrieve typename from type cache");
        }
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
    public String explain(@Nonnull final Formatter formatter) {
        return "(" +
               childrenAndNames.stream()
                       .map(childAndName -> {
                           if (childAndName.getValue().isPresent()) {
                               return childAndName.getKey().explain(formatter) + " as " + childAndName.getValue().get();
                           }
                           return childAndName.getKey().explain(formatter);
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
    public TupleConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(childrenAndNames.size() == Iterables.size(newChildren));
        //noinspection UnstableApiUsage
        final ImmutableList<Pair<? extends Value, Optional<String>>> newChildrenAndNames =
                Streams.zip(StreamSupport.stream(newChildren.spliterator(), false),
                        this.childrenAndNames.stream(),
                        (newChild, childAndName) -> Pair.of(newChild, childAndName.getValue()))
                        .collect(ImmutableList.toImmutableList());

        return new TupleConstructorValue(newChildrenAndNames);
    }

    public static List<? extends Value> tryUnwrapIfTuple(@Nonnull final List<? extends Value> values) {
        if (values.size() != 1) {
            return values;
        }

        final Value onlyElement = Iterables.getOnlyElement(values);
        if (!(onlyElement instanceof TupleConstructorValue)) {
            return values;
        }

        return ImmutableList.copyOf(onlyElement.getChildren());
    }

    public static TupleConstructorValue wrapIfNotTuple(@Nonnull final Value value) {
        if (value instanceof TupleConstructorValue) {
            return (TupleConstructorValue)value;
        }

        return TupleConstructorValue.ofUnnamed(value);
    }

    private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Atom> arguments) {
        final ImmutableList<Pair<? extends Value, Optional<String>>> namedArguments =
                arguments.stream()
                        .map(atom -> Pair.of(atom.narrow(Value.class), Optional.<String>empty()))
                        .peek(pair -> Verify.verify(pair.getKey().isPresent()))
                        .map(pair -> Pair.of(pair.getKey().get(), pair.getValue()))
                        .collect(ImmutableList.toImmutableList());
        return new TupleConstructorValue(namedArguments);
    }

    public static TupleConstructorValue of(@Nonnull final List<Pair<? extends Value, Optional<String>>> namedArguments) {
        return new TupleConstructorValue(namedArguments);
    }

    public static TupleConstructorValue of(@Nonnull final Pair<? extends Value, Optional<String>> namedArgument) {
        return new TupleConstructorValue(ImmutableList.of(namedArgument));
    }

    public static TupleConstructorValue ofUnnamed(@Nonnull final Value argument) {
        return new TupleConstructorValue(ImmutableList.of(Pair.of(argument, Optional.empty())));
    }

    public static TupleConstructorValue ofUnnamed(@Nonnull final Collection<? extends Value> arguments) {
        return new TupleConstructorValue(arguments.stream()
                        .map(argument -> Pair.of(argument, Optional.<String>empty()))
                        .collect(ImmutableList.toImmutableList()));
    }

    @AutoService(BuiltInFunction.class)
    public static class TupleFn extends BuiltInFunction<Value> {
        public TupleFn() {
            super("tuple",
                    ImmutableList.of(), new Type.Any(), TupleConstructorValue::encapsulate);
        }
    }
}
