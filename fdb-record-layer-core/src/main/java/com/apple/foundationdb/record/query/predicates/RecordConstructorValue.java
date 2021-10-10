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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.norse.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordConstructorValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Constructor-Value");

    @Nonnull
    private final String protoTypeName;

    @Nonnull
    private final Map<String, ? extends Value> keyChildrenMap;

    @Nonnull
    private final Map<String, Type> keyTypeMap;

    @Nonnull
    private final Supplier<Type> resultTypeSupplier;

    private RecordConstructorValue(@Nonnull String protoTypeName, @Nonnull final Map<String, ? extends Value> keyChildrenMap, @Nonnull final Map<String, Type> keyTypeMap) {
        this.protoTypeName = protoTypeName;
        this.keyChildrenMap = ImmutableMap.copyOf(keyChildrenMap);
        this.keyTypeMap = ImmutableMap.copyOf(keyTypeMap);
        this.resultTypeSupplier = Suppliers.memoize(() -> Type.Record.fromTypeMap(keyTypeMap));
    }

    @Nonnull
    public String getProtoTypeName() {
        return protoTypeName;
    }

    @Nullable
    @Override
    @SuppressWarnings({"java:S6213", "unchecked"})
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        final DynamicSchema dynamicSchema = context.getDynamicSchema();
        final DynamicMessage.Builder resultMessageBuilder = dynamicSchema.newMessageBuilder(protoTypeName);
        final Descriptors.Descriptor descriptorForType = resultMessageBuilder.getDescriptorForType();

        keyChildrenMap.forEach((key, child) -> {
            Object childResult = child.eval(store, context, record, message);
            if (childResult != null) {
                // craziness abounds as our own internal records are more than just of type Message
                if (childResult instanceof FDBRecord) {
                    childResult = ((FDBRecord<?>)childResult).getRecord();
                }

                final Descriptors.FieldDescriptor fieldDescriptor = descriptorForType.findFieldByName(key);

//                final Type childResultType = child.getResultType();
//                if (childResultType.getTypeCode() == Type.TypeCode.ARRAY && childResultType.isNullable()) {
//                    final Collection<Object> childResultCollection = (Collection<Object>)childResult;
//                    final Descriptors.Descriptor helperDescriptor = fieldDescriptor.getMessageType();
//
//                    childResult =
//                            childResultCollection
//                                    .stream()
//                                    .map(childResultElement -> {
//                                        final DynamicMessage.Builder helperMessageBuilder = DynamicMessage.newBuilder(helperDescriptor);
//                                        helperMessageBuilder.setField(helperDescriptor.findFieldByNumber(1), childResultElement);
//                                        return helperMessageBuilder.build();
//                                    })
//                                    .collect(ImmutableList.toImmutableList());
//                }

                resultMessageBuilder.setField(fieldDescriptor, childResult);
            }
        });

        return resultMessageBuilder.build();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "{" + keyChildrenMap.entrySet()
                .stream()
                .map(entry -> entry.getKey() + " -> " + entry.getValue().explain(formatter))
                .collect(Collectors.joining(", ")) + "}";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultTypeSupplier.get();
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return keyChildrenMap.values();
    }

    @Nonnull
    @Override
    public RecordConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
        final ImmutableMap.Builder<String, Value> newKeyChildrenMapBuilder = ImmutableMap.builder();
        final Iterator<? extends Map.Entry<String, ? extends Value>> entryIterator = keyChildrenMap.entrySet().iterator();
        for (final Value newChild : newChildren) {
            Verify.verify(entryIterator.hasNext());
            final Map.Entry<String, ? extends Value> entry = entryIterator.next();
            final Value child = entry.getValue();
            Verify.verify(child.getResultType().equals(newChild.getResultType()));
            newKeyChildrenMapBuilder.put(entry.getKey(), newChild);
        }
        Verify.verify(!entryIterator.hasNext());

        return new RecordConstructorValue(protoTypeName, newKeyChildrenMapBuilder.build(), keyTypeMap);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, keyChildrenMap);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, keyChildrenMap);
    }

    @Override
    public String toString() {
        return "{ " + keyChildrenMap.entrySet()
                .stream()
                .map(entry -> entry.getKey() + " -> " + entry.getValue())
                .collect(Collectors.joining(", ")) + " }";
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

    public static RecordConstructorValue createAndRegister(@Nonnull DynamicSchema.Builder dynamicSchemaBuilder, @Nonnull final Map<String, ? extends Value> keyChildrenMap) {
        final ImmutableMap.Builder<String, Type> keyTypeMapBuilder = ImmutableMap.builder();
        for (final Map.Entry<String, ? extends Value> entry : keyChildrenMap.entrySet()) {
            keyTypeMapBuilder.put(entry.getKey(), entry.getValue().getResultType());
        }
        final ImmutableMap<String, Type> keyTypeMap = keyTypeMapBuilder.build();
        final RecordConstructorValue recordConstructorValue = new RecordConstructorValue(Type.uniqueCompliantTypeName(), keyChildrenMap, keyTypeMap);

        dynamicSchemaBuilder.addType(recordConstructorValue.getProtoTypeName(), recordConstructorValue.getResultType());
        return recordConstructorValue;
    }
}
