/*
 * AutoTestDescriptor.java
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

import com.apple.foundationdb.relational.autotest.Connector;
import com.apple.foundationdb.relational.autotest.DataSet;
import com.apple.foundationdb.relational.autotest.SchemaDescription;
import com.apple.foundationdb.relational.autotest.WorkloadConfig;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.TestInstances;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.JunitUtils;
import org.junit.jupiter.engine.execution.InterceptingExecutableInvoker;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.jupiter.engine.extension.ExtensionRegistry;
import org.junit.jupiter.engine.extension.MutableExtensionRegistry;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.hierarchical.ThrowableCollector;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.engine.descriptor.TestFactoryTestDescriptor.DYNAMIC_CONTAINER_SEGMENT_TYPE;

class AutoTestDescriptor extends ClassTestDescriptor {

    private static final InterceptingExecutableInvoker executableInvoker = new InterceptingExecutableInvoker();
    private static final InterceptingExecutableInvoker.ReflectiveInterceptorCall<Method, Object> interceptorCall = InvocationInterceptor::interceptTestFactoryMethod;

    private final SchemaInvoker schemaInvoker;
    private final DataInvoker dataInvoker;
    private final QueryInvoker queryInvoker;
    private final List<ConnectionMaker> connectionMakers;
    private final ConfigurationInvoker configInvoker;
    private static final WorkloadConfig defaultConfig = new WorkloadConfig(
            System.getProperty("user.dir"),
            Map.of(
                    WorkloadConfig.INSERT_BATCH_SIZE, 16,
                    WorkloadConfig.SAMPLE_SIZE, 128
    ));

    AutoTestDescriptor(UniqueId uniqueId,
                       Class<?> testClass,
                       JupiterConfiguration configuration,
                       List<ConnectionMaker> connectionMakers,
                       SchemaInvoker schemaInvoker,
                       DataInvoker dataInvoker,
                       QueryInvoker queryInvoker,
                       ConfigurationInvoker configInvoker) {
        super(uniqueId, testClass, configuration);
        this.connectionMakers = connectionMakers;
        this.schemaInvoker = schemaInvoker;
        this.dataInvoker = dataInvoker;
        this.queryInvoker = queryInvoker;
        this.configInvoker = configInvoker;
    }

    @Override
    public Type getType() {
        return Type.CONTAINER;
    }

    @Override
    public boolean mayRegisterTests() {
        return true;
    }

    @Override
    public JupiterEngineExecutionContext execute(JupiterEngineExecutionContext context, DynamicTestExecutor dynamicTestExecutor) throws Exception {
        JupiterConfiguration configuration = context.getConfiguration();

        final ThrowableCollector throwableCollector = context.getThrowableCollector();
        throwableCollector.execute(() -> {
            invokeBeforeEachCallbacks(context);
            if (throwableCollector.isEmpty()) {
                invokeWorkloadTests(context, dynamicTestExecutor, configuration);
            }
            invokeAfterEachCallbacks(context);
        });
        return context;
    }

    private void invokeBeforeEachCallbacks(JupiterEngineExecutionContext context)  throws Exception {
        MutableExtensionRegistry registry = context.getExtensionRegistry();
        ExtensionContext extensionContext = context.getExtensionContext();
        ThrowableCollector throwableCollector = context.getThrowableCollector();
        final TestInstances testInstances = context.getTestInstancesProvider().getTestInstances(registry, throwableCollector);
        JunitUtils.setTestInstances(extensionContext, testInstances);

        for (BeforeEachCallback callback : registry.getExtensions(BeforeEachCallback.class)) {
            throwableCollector.execute(() -> callback.beforeEach(extensionContext));
            if (throwableCollector.isNotEmpty()) {
                break;
            }
        }
    }

    private void invokeAfterEachCallbacks(JupiterEngineExecutionContext context)  throws Exception {
        ExtensionRegistry registry = context.getExtensionRegistry();
        ExtensionContext extensionContext = context.getExtensionContext();
        ThrowableCollector throwableCollector = context.getThrowableCollector();

        for (AfterEachCallback callback : registry.getExtensions(AfterEachCallback.class)) {
            throwableCollector.execute(() -> callback.afterEach(extensionContext));
            if (throwableCollector.isNotEmpty()) {
                break;
            }
        }
    }

    @Override
    public void cleanUp(JupiterEngineExecutionContext context) throws Exception {
        context.getThrowableCollector().assertEmpty();
    }

    private void invokeWorkloadTests(JupiterEngineExecutionContext context, DynamicTestExecutor dynamicTestExecutor, JupiterConfiguration configuration) throws InterruptedException {
        Object instance = context.getTestInstancesProvider()
                .getTestInstances(context.getExtensionRegistry(), context.getThrowableCollector())
                .findInstance(getTestClass())
                .orElseThrow();

        List<Connector> connectors = connectionMakers.stream()
                .map(maker -> maker.createConnector(instance))
                .collect(Collectors.toList());
        Collection<DataSet> dataSets = dataInvoker.getDataSets(instance, context, executableInvoker);
        //TODO(bfines) deal with multiple config instances
        WorkloadConfig config = configInvoker.getConfigurations(instance, context, executableInvoker).stream().findFirst().orElse(defaultConfig);
        config.setDefaults(defaultConfig); //make sure the default values are set if they weren't overridden

        MutableExtensionRegistry autoTestRegistry = context.getExtensionRegistry();
        autoTestRegistry = MutableExtensionRegistry.createRegistryFrom(autoTestRegistry, Stream.empty());

        autoTestRegistry.registerExtension(config, instance);

        try (Stream<SchemaDescription> schemas = schemaInvoker.getSchemaDescriptions(instance, context, autoTestRegistry, executableInvoker)) {
            AtomicInteger counter = new AtomicInteger(1);
            UniqueId parentId = getUniqueId();
            schemas.map(schema -> {
                final int workloadInt = counter.getAndIncrement();
                URI dbPath = URI.create("/TEST/" + schema.getDisplayName() + "_" + workloadInt + "");
                return dataSets.stream().map(dataSet -> new AutoWorkload(schema, dataSet, dbPath, connectors, getTestClass().getSimpleName() + "." + workloadInt, config));
            }).forEach(workloadStream -> workloadStream.forEach(workload -> {
                Collection<QuerySet> queries = queryInvoker.getQueries(instance, workload.getSchema(), context, executableInvoker);
                queries.forEach(querySet -> {
                    UniqueId uid = parentId.append(DYNAMIC_CONTAINER_SEGMENT_TYPE, workload.getDisplayName() + "-" + querySet.getLabel());
                    TestDescriptor descriptor = new WorkloadTestDescriptor(uid, getTestClass(), configuration, workload, querySet);
                    dynamicTestExecutor.execute(descriptor);
                });
            }));
        }
        dynamicTestExecutor.awaitFinished();
    }
}
