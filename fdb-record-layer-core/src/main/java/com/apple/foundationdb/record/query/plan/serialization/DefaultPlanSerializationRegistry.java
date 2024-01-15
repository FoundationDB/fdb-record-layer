/*
 * DefaultPlanSerializationRegistry.java
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

package com.apple.foundationdb.record.query.plan.serialization;

import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Default implementation of a plan serialization registry.
 */
public class DefaultPlanSerializationRegistry implements PlanSerializationRegistry {
    public static final PlanSerializationRegistry INSTANCE = new DefaultPlanSerializationRegistry();
    private static final String TYPE_URL_PREFIX = "c.a.fdb.types";

    private final Map<Class<? extends Message>, PlanDeserializer<? extends Message, ?>> fromProtoClassDeserializerMap;
    private final Map<String, Class<? extends Message>> fromProtoTypeUrlClassMap;

    public DefaultPlanSerializationRegistry() {
        final var methodMaps = loadFromProtoMethodMaps();
        this.fromProtoClassDeserializerMap = methodMaps.getLeft();
        this.fromProtoTypeUrlClassMap = methodMaps.getRight();
    }

    @Nonnull
    @Override
    public String getTypeUrlPrefix() {
        return TYPE_URL_PREFIX;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> PlanDeserializer<M, ?> lookUpFromProto(@Nonnull final Class<M> messageClass) {
        final PlanDeserializer<M, ?> deserializer = (PlanDeserializer<M, ?>)fromProtoClassDeserializerMap.get(messageClass);
        if (deserializer == null) {
            throw new RecordCoreException("unable to dispatch for message of class " + messageClass);
        }
        return deserializer;
    }

    @Nonnull
    @Override
    public Class<? extends Message> lookUpMessageClass(@Nonnull final String typeUrl) {
        final Class<? extends Message> protoMessageClass = fromProtoTypeUrlClassMap.get(typeUrl);
        if (protoMessageClass == null) {
            throw new RecordCoreException("unable to dispatch for type url " + typeUrl);
        }
        return protoMessageClass;
    }

    @SuppressWarnings("unchecked")
    private Pair<Map<Class<? extends Message>, PlanDeserializer<? extends Message, ?>>, Map<String, Class<? extends Message>>> loadFromProtoMethodMaps() {
        final Iterable<PlanDeserializer<? extends Message, ?>> planDeserializers =
                (Iterable<PlanDeserializer<? extends Message, ?>>)(Object)ServiceLoaderProvider.load(PlanDeserializer.class);

        final ImmutableMap.Builder<Class<? extends Message>, PlanDeserializer<? extends Message, ?>> fromProtoClassDeserializerMapBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<String, Class<? extends Message>> fromProtoTypeUrlClassMapBuilder = ImmutableMap.builder();

        Streams.stream(planDeserializers)
                .forEach(planDeserializer -> {
                    final Class<? extends Message> protoMessageClass = planDeserializer.getProtoMessageClass();
                    fromProtoClassDeserializerMapBuilder.put(protoMessageClass, planDeserializer);

                    final Method getDescriptorMethod;
                    try {
                        getDescriptorMethod = protoMessageClass.getMethod("getDescriptor");
                    } catch (final NoSuchMethodException e) {
                        throw new RecordCoreException("unable to find getDescriptor() method", e);
                    }

                    final Descriptors.Descriptor protoMessageDescriptor;
                    try {
                        protoMessageDescriptor = (Descriptors.Descriptor)getDescriptorMethod.invoke(null);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RecordCoreException("unable to invoke getDescriptor Method", e);
                    }
                    fromProtoTypeUrlClassMapBuilder.put(getTypeUrl(protoMessageDescriptor), protoMessageClass);
                });
        return Pair.of(fromProtoClassDeserializerMapBuilder.build(), fromProtoTypeUrlClassMapBuilder.build());
    }

    @Nonnull
    private static String getTypeUrl(@Nonnull final Descriptors.Descriptor descriptor) {
        return TYPE_URL_PREFIX + "/" + descriptor.getFullName();
    }
}
