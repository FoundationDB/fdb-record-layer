/*
 * SchemaInvoker.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.autotest.SchemaDescription;

import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.engine.execution.InterceptingExecutableInvoker;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.jupiter.engine.extension.ExtensionRegistry;
import org.junit.platform.commons.JUnitException;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

class SchemaInvoker {
    private static final InterceptingExecutableInvoker.ReflectiveInterceptorCall<Method, Object> interceptorCall = InvocationInterceptor::interceptTestFactoryMethod;
    private final List<Method> schemaMethods;
    private final List<Field> schemaFields;

    public SchemaInvoker(List<Method> schemaMethods, List<Field> schemaFields) {
        this.schemaMethods = schemaMethods;
        this.schemaFields = schemaFields;
    }

    public Stream<SchemaDescription> getSchemaDescriptions(Object testInstance,
                                                           JupiterEngineExecutionContext context,
                                                           ExtensionRegistry extensionRegistry,
                                                           InterceptingExecutableInvoker executableInvoker) {
        Stream<SchemaDescription> fieldStream = getSchemasFromField(testInstance);
        Stream<SchemaDescription> methodStream = getSchemaFromMethod(testInstance, context, extensionRegistry, executableInvoker);

        return Stream.concat(fieldStream, methodStream);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Stream<SchemaDescription> getSchemaFromMethod(Object testInstance, JupiterEngineExecutionContext context, ExtensionRegistry registry, InterceptingExecutableInvoker executableInvoker) {
        if (schemaMethods.isEmpty()) {
            return Stream.empty();
        }
        List<SchemaDescription> singleDescriptions = new ArrayList<>();
        Stream<SchemaDescription> descriptionStream = null;
        for (Method method : schemaMethods) {
            try {
                Object result = executableInvoker.invoke(method, testInstance, context.getExtensionContext(), registry, interceptorCall);

                if (result instanceof SchemaDescription) {
                    singleDescriptions.add((SchemaDescription) result);
                } else if (result instanceof Collection) {
                    singleDescriptions.addAll((Collection<SchemaDescription>) result);
                } else if (result instanceof Stream) {
                    if (descriptionStream == null) {
                        descriptionStream = (Stream<SchemaDescription>) result;
                    } else {
                        descriptionStream = Stream.concat(descriptionStream, (Stream<SchemaDescription>) result);
                    }
                } else {
                    throw new ClassCastException();
                }
            } catch (ClassCastException cce) {
                String message = String.format(Locale.ROOT, "Method [%s] must return a Stream<SchemaDescription>,Collection<SchemaDescription>, or SchemaDescription", method.getName());
                throw new JUnitException(message, cce);
            }
        }

        if (!singleDescriptions.isEmpty()) {
            if (descriptionStream != null) {
                descriptionStream = Stream.concat(descriptionStream, singleDescriptions.stream());
            } else {
                descriptionStream = singleDescriptions.stream();
            }
        }
        assert descriptionStream != null : "Programmer error, stream shouldn't be null!";
        return descriptionStream;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Stream<SchemaDescription> getSchemasFromField(Object testInstance) {
        if (schemaFields.isEmpty()) {
            return Stream.empty();
        }
        List<SchemaDescription> singleDescriptions = new ArrayList<>();
        Stream<SchemaDescription> descriptionStream = null;
        for (Field field :schemaFields) {
            try {
                Object o = field.get(testInstance);
                if (o instanceof SchemaDescription) {
                    singleDescriptions.add((SchemaDescription) o);
                } else if (o instanceof Collection) {
                    singleDescriptions.addAll((Collection<SchemaDescription>) o);
                } else if (o instanceof Stream) {
                    if (descriptionStream == null) {
                        descriptionStream = (Stream<SchemaDescription>) o;
                    } else {
                        descriptionStream = Stream.concat(descriptionStream, (Stream<SchemaDescription>) o);
                    }
                } else {
                    throw new ClassCastException();
                }
            } catch (IllegalAccessException e) {
                throw new JUnitException(String.format(Locale.ROOT, "Field [%s] must be public", field.getName()), e);
            } catch (ClassCastException cce) {
                String message = String.format(Locale.ROOT, "Field [%s] must be a Stream<SchemaDescription>,Collection<SchemaDescription>, or SchemaDescription", field.getName());
                throw new JUnitException(message, cce);
            }
        }

        if (!singleDescriptions.isEmpty()) {
            if (descriptionStream != null) {
                descriptionStream = Stream.concat(descriptionStream, singleDescriptions.stream());
            } else {
                descriptionStream = singleDescriptions.stream();
            }
        }
        assert descriptionStream != null : "Programmer error, stream shouldn't be null!";
        return descriptionStream;
    }
}
