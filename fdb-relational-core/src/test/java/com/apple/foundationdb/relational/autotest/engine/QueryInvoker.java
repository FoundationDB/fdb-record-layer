/*
 * QueryInvoker.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.autotest.engine;

import com.apple.foundationdb.relational.autotest.ParameterizedQuery;
import com.apple.foundationdb.relational.autotest.Query;
import com.apple.foundationdb.relational.autotest.SchemaDescription;

import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.engine.execution.InterceptingExecutableInvoker;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.jupiter.engine.extension.MutableExtensionRegistry;
import org.junit.platform.commons.JUnitException;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

class QueryInvoker {
    private static final InterceptingExecutableInvoker.ReflectiveInterceptorCall<Method, Object> interceptorCall = InvocationInterceptor::interceptTestFactoryMethod;
    private final List<Method> schemaMethods;
    private final List<Field> schemaFields;

    public QueryInvoker(List<Method> schemaMethods, List<Field> schemaFields) {
        this.schemaMethods = schemaMethods;
        this.schemaFields = schemaFields;
    }

    public Collection<QuerySet> getQueries(Object testInstance,
                                           SchemaDescription schemaDescription,
                                           JupiterEngineExecutionContext context,
                                           InterceptingExecutableInvoker executableInvoker) {
        Collection<QuerySet> fieldStream = getQueriessFromField(testInstance);
        Collection<QuerySet> methodStream = getQueriesFromMethod(testInstance, schemaDescription, context, executableInvoker);

        if (fieldStream.isEmpty()) {
            return methodStream;
        } else if (methodStream.isEmpty()) {
            return fieldStream;
        } else {
            fieldStream.addAll(methodStream);
            return fieldStream;
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Collection<QuerySet> getQueriesFromMethod(Object testInstance,
                                                      SchemaDescription description,
                                                      JupiterEngineExecutionContext context,
                                                      InterceptingExecutableInvoker executableInvoker) {
        if (schemaMethods.isEmpty()) {
            return Collections.emptyList();
        }
        MutableExtensionRegistry extensionRegistry = context.getExtensionRegistry();
        extensionRegistry = MutableExtensionRegistry.createRegistryFrom(extensionRegistry, Stream.empty());
        extensionRegistry.registerExtension(new SchemaParameterResolver(description), testInstance);
        List<QuerySet> querySets = new ArrayList<>();
        for (Method method : schemaMethods) {
            List<ParameterizedQuery> queries = new ArrayList<>();
            String label = method.getAnnotation(Query.class).label();
            try {
                Object result = executableInvoker.invoke(method, testInstance, context.getExtensionContext(), extensionRegistry, interceptorCall);

                if (result instanceof ParameterizedQuery) {
                    queries.add((ParameterizedQuery) result);
                } else if (result instanceof Collection) {
                    queries.addAll((Collection<ParameterizedQuery>) result);
                } else if (result instanceof Stream) {
                    ((Stream<ParameterizedQuery>) result).forEach(queries::add);
                } else {
                    throw new ClassCastException();
                }
            } catch (ClassCastException cce) {
                String message = String.format(Locale.ROOT, "Method [%s] must return a Stream<ParameterizedQuery>,Collection<ParameterizedQuery>, or ParameterizedQuery", method.getName());
                throw new JUnitException(message, cce);
            }
            querySets.add(new QuerySet(queries, label));
        }

        return querySets;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Collection<QuerySet> getQueriessFromField(Object testInstance) {
        if (schemaFields.isEmpty()) {
            return Collections.emptyList();
        }
        Collection<QuerySet> querySets = new ArrayList<>();
        for (Field field :schemaFields) {
            List<ParameterizedQuery> queries = new ArrayList<>();
            String label = field.getAnnotation(Query.class).label();
            try {
                Object result = field.get(testInstance);
                if (result instanceof ParameterizedQuery) {
                    queries.add((ParameterizedQuery) result);
                } else if (result instanceof Collection) {
                    queries.addAll((Collection<ParameterizedQuery>) result);
                } else if (result instanceof Stream) {
                    ((Stream<ParameterizedQuery>) result).forEach(queries::add);
                } else {
                    throw new ClassCastException();
                }
            } catch (IllegalAccessException e) {
                throw new JUnitException(String.format(Locale.ROOT, "Field [%s] must be public", field.getName()), e);
            } catch (ClassCastException cce) {

                String message = String.format(Locale.ROOT, "Field [%s] must be a Stream<ParameterizedQuery>,Collection<ParameterizedQuery>, or ParameterizedQuery", field.getName());
                throw new JUnitException(message, cce);
            }
            querySets.add(new QuerySet(queries, label));
        }
        return querySets;
    }
}
