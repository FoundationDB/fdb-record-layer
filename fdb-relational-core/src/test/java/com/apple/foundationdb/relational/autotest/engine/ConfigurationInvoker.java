/*
 * ConfigurationInvoker.java
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

package com.apple.foundationdb.relational.autotest.engine;

import com.apple.foundationdb.relational.autotest.WorkloadConfig;

import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.engine.execution.InterceptingExecutableInvoker;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.platform.commons.JUnitException;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

class ConfigurationInvoker {
    private static final InterceptingExecutableInvoker.ReflectiveInterceptorCall<Method, Object> interceptorCall = InvocationInterceptor::interceptTestFactoryMethod;
    private final List<Method> schemaMethods;
    private final List<Field> schemaFields;

    public ConfigurationInvoker(List<Method> schemaMethods, List<Field> schemaFields) {
        this.schemaMethods = schemaMethods;
        this.schemaFields = schemaFields;
    }

    public Collection<WorkloadConfig> getConfigurations(Object testInstance,
                                                        JupiterEngineExecutionContext context,
                                                        InterceptingExecutableInvoker executableInvoker) {
        Collection<WorkloadConfig> fieldStream = getSchemasFromField(testInstance);
        Collection<WorkloadConfig> methodStream = getSchemaFromMethod(testInstance, context, executableInvoker);

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
    private Collection<WorkloadConfig> getSchemaFromMethod(Object testInstance, JupiterEngineExecutionContext context, InterceptingExecutableInvoker executableInvoker) {
        if (schemaMethods.isEmpty()) {
            return Collections.emptyList();
        }
        List<WorkloadConfig> singleDescriptions = new ArrayList<>();
        for (Method method : schemaMethods) {
            try {
                Object result = executableInvoker.invoke(method, testInstance, context.getExtensionContext(), context.getExtensionRegistry(), interceptorCall);

                if (result instanceof WorkloadConfig) {
                    singleDescriptions.add((WorkloadConfig) result);
                } else if (result instanceof Collection) {
                    singleDescriptions.addAll((Collection<WorkloadConfig>) result);
                } else if (result instanceof Stream) {
                    ((Stream<WorkloadConfig>) result).forEach(singleDescriptions::add);
                } else {
                    throw new ClassCastException();
                }
            } catch (ClassCastException cce) {
                String message = String.format("Method [%s] must return a Stream<WorkloadConfig>,Collection<WorkloadConfig>, or WorkloadConfig", method.getName());
                throw new JUnitException(message, cce);
            }
        }

        return singleDescriptions;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Collection<WorkloadConfig> getSchemasFromField(Object testInstance) {
        if (schemaFields.isEmpty()) {
            return Collections.emptyList();
        }
        List<WorkloadConfig> singleDescriptions = new ArrayList<>();
        for (Field field :schemaFields) {
            try {
                Object result = field.get(testInstance);
                if (result instanceof WorkloadConfig) {
                    singleDescriptions.add((WorkloadConfig) result);
                } else if (result instanceof Collection) {
                    singleDescriptions.addAll((Collection<WorkloadConfig>) result);
                } else if (result instanceof Stream) {
                    ((Stream<WorkloadConfig>) result).forEach(singleDescriptions::add);
                } else {
                    throw new ClassCastException();
                }
            } catch (IllegalAccessException e) {
                throw new JUnitException(String.format("Field [%s] must be public", field.getName()), e);
            } catch (ClassCastException cce) {
                String message = String.format("Field [%s] must be a Stream<WorkloadConfig>,Collection<WorkloadConfig>, or WorkloadConfig", field.getName());
                throw new JUnitException(message, cce);
            }
        }
        return singleDescriptions;
    }
}
