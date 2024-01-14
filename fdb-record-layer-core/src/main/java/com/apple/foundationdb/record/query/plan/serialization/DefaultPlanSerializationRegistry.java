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

import com.apple.foundationdb.annotation.ProtoMessage;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Default implementation of a plan serialization registry.
 */
public class DefaultPlanSerializationRegistry implements PlanSerializationRegistry {
    public static final PlanSerializationRegistry INSTANCE = new DefaultPlanSerializationRegistry();

    private static final Logger logger = LoggerFactory.getLogger(DefaultPlanSerializationRegistry.class);

    private static final String PREFIX = "META-INF/services/";
    private static final String TYPE_URL_PREFIX = "c.a.fdb.types";

    private final Map<Class<? extends Message>, Method> fromProtoClassMethodMap;
    private final Map<String, Class<? extends Message>> fromProtoTypeUrlClassMap;

    public DefaultPlanSerializationRegistry() {
        final Pair<Map<Class<? extends Message>, Method>, Map<String, Class<? extends Message>>> methodMaps = loadFromProtoMethodMaps();
        this.fromProtoClassMethodMap = methodMaps.getLeft();
        this.fromProtoTypeUrlClassMap = methodMaps.getRight();
    }

    @Nonnull
    @Override
    public String getTypeUrlPrefix() {
        return TYPE_URL_PREFIX;
    }

    @Nonnull
    @Override
    public Method lookUpFromProto(@Nonnull final Class<? extends Message> messageClass) {
        final Method fromProtoMethod = fromProtoClassMethodMap.get(messageClass);
        if (fromProtoMethod == null) {
            throw new RecordCoreException("unable to dispatch for message of class " + messageClass);
        }
        return fromProtoMethod;
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

    /**
     * Parse a single line from the given configuration file, adding the
     * name on the line to set of names if not already seen.
     */
    private static int parseLine(BufferedReader r, int lc, Set<String> namesAlreadySeen, Set<String> names) throws IOException {
        String ln = r.readLine();
        if (ln == null) {
            return -1;
        }
        int ci = ln.indexOf('#');
        if (ci >= 0) {
            ln = ln.substring(0, ci);
        }
        ln = ln.trim();
        int n = ln.length();
        if (n != 0) {
            if ((ln.indexOf(' ') >= 0) || (ln.indexOf('\t') >= 0)) {
                throw new RecordCoreException("Illegal configuration-file syntax");
            }
            int cp = ln.codePointAt(0);
            if (!Character.isJavaIdentifierStart(cp)) {
                throw new RecordCoreException("Illegal provider-class name: " + ln);
            }
            int start = Character.charCount(cp);
            for (int i = start; i < n; i += Character.charCount(cp)) {
                cp = ln.codePointAt(i);
                if (!Character.isJavaIdentifierPart(cp) && (cp != '.')) {
                    throw new RecordCoreException("Illegal provider-class name: " + ln);
                }
            }
            if (namesAlreadySeen.add(ln)) {
                names.add(ln);
            }
        }
        return lc + 1;
    }

    /**
     * Parse the content of the given URL as a provider-configuration file.
     */
    @Nonnull
    private static Stream<String> parse(final URL u, final Set<String> namesAlreadySeen) {
        Set<String> names = new LinkedHashSet<>(); // preserve insertion order
        try {
            URLConnection uc = u.openConnection();
            uc.setUseCaches(false);
            try (InputStream in = uc.getInputStream();
                    BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)))
            {
                int lc = 1;
                do {
                    lc = parseLine(r, lc, namesAlreadySeen, names);
                } while (lc > 0);
            }
        } catch (IOException x) {
            throw new RecordCoreException("Error accessing configuration file", x);
        }
        return names.stream();
    }

    @SuppressWarnings({"UnstableApiUsage", "unchecked"})
    private Pair<Map<Class<? extends Message>, Method>, Map<String, Class<? extends Message>>> loadFromProtoMethodMaps() {
        Enumeration<URL> configs;
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();

        try {
            String fullName = PREFIX + PlanSerializable.class.getName();
            if (loader == null) {
                configs = ClassLoader.getSystemResources(fullName);
            } else {
                configs = loader.getResources(fullName);
            }
        } catch (IOException x) {
            throw new RecordCoreException("Error locating configuration files", x);
        }

        Set<String> namesAlreadySeen = Sets.newHashSet();

        final ImmutableMap.Builder<Class<? extends Message>, Method> fromProtoClassMethodMapBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<String, Class<? extends Message>> fromProtoTypeUrlClassMapBuilder = ImmutableMap.builder();

        Streams.stream(configs.asIterator())
                .flatMap(config -> parse(config, namesAlreadySeen))
                .flatMap(className -> {
                    try {
                        return Stream.of(Class.forName(className));
                    } catch (ClassNotFoundException e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("unable to find class for class name {}", className);
                        }
                    }
                    return Stream.empty();
                })
                .forEach(clazz -> {
                    if (!clazz.isAnnotationPresent(ProtoMessage.class)) {
                        return;
                    }
                    final ProtoMessage annotation = Objects.requireNonNull(clazz.getAnnotation(ProtoMessage.class));
                    if (!Message.class.isAssignableFrom(annotation.value())) {
                        throw new RecordCoreException("unsupported serialization class");
                    }
                    final Method fromProtoMethod;
                    try {
                        fromProtoMethod =
                                clazz.getMethod("fromProto", PlanSerializationContext.class, annotation.value());
                    } catch (final NoSuchMethodException e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("unable to find static method {}.fromProto(...)", clazz.getSimpleName());
                        }
                        return;
                    }

                    final Class<? extends Message> protoMessageClass = (Class<? extends Message>)annotation.value();
                    fromProtoClassMethodMapBuilder.put(protoMessageClass, fromProtoMethod);

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
        return Pair.of(fromProtoClassMethodMapBuilder.build(), fromProtoTypeUrlClassMapBuilder.build());
    }

    @Nonnull
    private static String getTypeUrl(@Nonnull final Descriptors.Descriptor descriptor) {
        return TYPE_URL_PREFIX + "/" + descriptor.getFullName();
    }
}
