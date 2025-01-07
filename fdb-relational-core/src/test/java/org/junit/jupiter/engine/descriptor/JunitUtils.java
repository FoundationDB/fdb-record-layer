/*
 * JunitUtils.java
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

package org.junit.jupiter.engine.descriptor;

import com.apple.foundationdb.annotation.API;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstances;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.extension.ExtensionRegistry;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.hierarchical.ThrowableCollector;

import java.util.Optional;

/**
 * Package-private hacks.
 */
@API(API.Status.EXPERIMENTAL)
public class JunitUtils {

    public static Optional<JupiterTestDescriptor> createDynamicDescriptor(JupiterTestDescriptor parent,
                                                                          DynamicNode node,
                                                                          int index,
                                                                          TestSource defaultTestSource,
                                                                          DynamicDescendantFilter descendantFilter,
                                                                          JupiterConfiguration configuration) {
        return TestFactoryTestDescriptor.createDynamicDescriptor(parent, node, index, defaultTestSource, descendantFilter, configuration);
    }

    public static void setTestInstances(ExtensionContext extensionContext, TestInstances testInstances) {
        if (extensionContext instanceof ClassExtensionContext) {
            ((ClassExtensionContext) extensionContext).setTestInstances(testInstances);
        }
    }

    public static ExtensionContext newClassExecutionContext(ExtensionContext parent, EngineExecutionListener listener,
                                                            ClassBasedTestDescriptor testDescriptor, JupiterConfiguration config,
                                                            ThrowableCollector collector) {
        return newClassExecutionContext(parent, listener, testDescriptor, config, collector);
    }

    public interface ExceptionHandlerInvoker<E extends Extension> {
        void invoke(E exceptionHandler, Throwable throwable) throws Throwable;
    }

    public static <E extends Extension> void  invokeExecutionExceptionHandlers(JupiterTestDescriptor testDescriptor,
                                                                               Class<E> handlerType,
                                                                               ExtensionRegistry registry,
                                                                               Throwable t,
                                                                               JunitUtils.ExceptionHandlerInvoker<E> handlerInvoker) {
        testDescriptor.invokeExecutionExceptionHandlers(handlerType, registry, t, handlerInvoker::invoke);
    }

    private JunitUtils() {
    }
}
