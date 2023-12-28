/*
 * PlanSerialization.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.FunctionCatalog;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * Class to facilitate serialization and deserialization of
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s.
 */
public class PlanSerialization {
    private static final Logger logger = LoggerFactory.getLogger(FunctionCatalog.class);

    private static final String PREFIX = "META-INF/services/";

    private static final Map<Class<? extends Message>, Method> fromProtoMapMethodMap =
            loadFromProtoMethodMap();

    /**
     * Parse a single line from the given configuration file, adding the
     * name on the line to set of names if not already seen.
     */
    private static int parseLine(BufferedReader r, int lc, Set<String> namesAlreadySeen, Set<String> names)
            throws IOException
    {
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
    @SuppressWarnings("StatementWithEmptyBody")
    private static Stream<String> parse(final URL u, final Set<String> namesAlreadySeen) {
        Set<String> names = new LinkedHashSet<>(); // preserve insertion order
        try {
            URLConnection uc = u.openConnection();
            uc.setUseCaches(false);
            try (InputStream in = uc.getInputStream();
                     BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)))
            {
                int lc = 1;
                while ((lc = parseLine(r, lc, namesAlreadySeen, names)) >= 0) {
                    // nothing
                }
            }
        } catch (IOException x) {
            throw new RecordCoreException("Error accessing configuration file", x);
        }
        return names.stream();
    }

    @SuppressWarnings({"UnstableApiUsage", "unchecked"})
    private static Map<Class<? extends Message>, Method> loadFromProtoMethodMap() {
        Enumeration<URL> configs;
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        //if (configs == null) {
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
        //}

        Set<String> namesAlreadySeen = Sets.newHashSet();

        return Streams.stream(configs.asIterator())
                .flatMap(config -> parse(config, namesAlreadySeen))
                .flatMap(className -> {
                    try {
                        return Stream.of(Class.forName(className));
                    } catch (ClassNotFoundException e) {
                        logger.warn("unable to find class for class name {}", className);
                    }
                    return Stream.empty();
                })
                .flatMap(clazz -> {
                    try {
                        if (!clazz.isAnnotationPresent(ProtoMessage.class)) {
                            return Stream.empty();
                        }
                        final ProtoMessage annotation = Objects.requireNonNull(clazz.getAnnotation(ProtoMessage.class));
                        if (!Message.class.isAssignableFrom(annotation.value())) {
                            throw new RecordCoreException("unsupported serialization class");
                        }
                        return Stream.of(Pair.of((Class<? extends Message>)annotation.value(),
                                clazz.getMethod("fromProto", PlanSerializationContext.class, annotation.value())));
                    } catch (final NoSuchMethodException e) {
                        logger.warn("unable to find static method {}.fromProto(...)", clazz.getSimpleName());
                    }
                    return Stream.empty();
                })
                .collect(ImmutableMap.toImmutableMap(p -> Objects.requireNonNull(p).getKey(), p -> Objects.requireNonNull(p).getValue()));
    }

    @Nonnull
    public static RecordMetaDataProto.Value valueObjectToProto(@Nullable final Object object) {
        return LiteralKeyExpression.toProtoValue(object);
    }

    @Nullable
    public static Object protoObjectToValue(@Nonnull final RecordMetaDataProto.Value proto) {
        return LiteralKeyExpression.fromProtoValue(proto);
    }

    @Nonnull
    public static Object dispatchFromProtoContainer(@Nonnull PlanSerializationContext serializationContext, @Nonnull final Message message) {
        final Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();
        Verify.verify(allFields.size() == 1);
        final Message field = (Message)Iterables.getOnlyElement(allFields.values());
        return PlanSerialization.dispatchFromProto(serializationContext, field);
    }

    @Nonnull
    public static Object dispatchFromProto(@Nonnull PlanSerializationContext serializationContext, @Nonnull final Message message) {
        final Method fromProtoMethod = fromProtoMapMethodMap.get(message.getClass());
        if (fromProtoMethod == null) {
            throw new RecordCoreException("unable to dispatch for message of class {}", message.getClass());
        }
        try {
            return Objects.requireNonNull(fromProtoMethod.invoke(null, serializationContext, message));
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RecordCoreException("unable to invoke fromProto method", e);
        }
    }
}
