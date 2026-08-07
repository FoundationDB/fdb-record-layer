/*
 * AutoTestEngine.java
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

import com.apple.foundationdb.relational.autotest.AutomatedTest;
import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.DefaultJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.jupiter.engine.execution.LauncherStoreFacade;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.platform.engine.DiscoveryIssue;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.ClassSelector;
import org.junit.platform.engine.discovery.ClasspathRootSelector;
import org.junit.platform.engine.discovery.PackageSelector;
import org.junit.platform.engine.support.discovery.DiscoveryIssueReporter;
import org.junit.platform.engine.support.hierarchical.HierarchicalTestEngine;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Optional;
import java.util.function.Predicate;

public class AutoTestEngine extends HierarchicalTestEngine<JupiterEngineExecutionContext> {
    private static final Predicate<Class<?>> IS_AUTO_TEST_CONTAINER
            = classCandidate -> AnnotationSupport.isAnnotated(classCandidate, AutomatedTest.class);

    @Override
    public String getId() {
        return "auto-test";
    }

    @Override
    public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId) {
        DiscoveryIssueReporter issueReporter = DiscoveryIssueReporter.deduplicating(
                DiscoveryIssueReporter.forwarding(discoveryRequest.getDiscoveryListener(), uniqueId));
        JupiterConfiguration config = new CachingJupiterConfiguration(
                new DefaultJupiterConfiguration(discoveryRequest.getConfigurationParameters(),
                        discoveryRequest.getOutputDirectoryCreator(), issueReporter));
        TestDescriptor rootDescriptor = new JupiterEngineDescriptor(uniqueId, config);

        discoveryRequest.getSelectorsByType(ClasspathRootSelector.class).forEach(selector ->
                appendTestsInClasspathRoot(selector.getClasspathRoot(), rootDescriptor, config, issueReporter));

        discoveryRequest.getSelectorsByType(PackageSelector.class).forEach(selector ->
                appendTestsInPackage(selector.getPackageName(), rootDescriptor, config, issueReporter));

        discoveryRequest.getSelectorsByType(ClassSelector.class).forEach(selector ->
                appendTestsInClass(selector.getJavaClass(), rootDescriptor, config, issueReporter));
        return rootDescriptor;
    }

    @Override
    protected JupiterEngineExecutionContext createExecutionContext(ExecutionRequest request) {
        JupiterEngineDescriptor engineDescriptor = (JupiterEngineDescriptor) request.getRootTestDescriptor();
        JupiterConfiguration config = engineDescriptor.getConfiguration();
        return new JupiterEngineExecutionContext(request.getEngineExecutionListener(), config,
                new LauncherStoreFacade(request.getStore()));
    }

    private void appendTestsInClass(Class<?> javaClass, TestDescriptor engineDesc, JupiterConfiguration config,
                                     DiscoveryIssueReporter issueReporter) {
        if (AnnotationSupport.isAnnotated(javaClass, AutomatedTest.class)) {
            getClassTestDescriptor(engineDesc, config, javaClass, issueReporter)
                    .ifPresent(engineDesc::addChild);
        }
    }

    private void appendTestsInPackage(String packageName, TestDescriptor engineDesc, JupiterConfiguration config,
                                       DiscoveryIssueReporter issueReporter) {
        ReflectionSupport.findAllClassesInPackage(packageName, IS_AUTO_TEST_CONTAINER, name -> true)
                .stream()
                .flatMap(clazz -> getClassTestDescriptor(engineDesc, config, clazz, issueReporter).stream())
                .forEach(engineDesc::addChild);
    }

    private void appendTestsInClasspathRoot(URI rootUri, TestDescriptor engineDesc, JupiterConfiguration config,
                                             DiscoveryIssueReporter issueReporter) {
        ReflectionSupport.findAllClassesInClasspathRoot(rootUri, IS_AUTO_TEST_CONTAINER, name -> true)
                .stream()
                .flatMap(aClass -> getClassTestDescriptor(engineDesc, config, aClass, issueReporter).stream())
                .forEach(engineDesc::addChild);
    }

    @Nonnull
    private Optional<TestDescriptor> getClassTestDescriptor(TestDescriptor engineDesc, JupiterConfiguration config,
                                                             Class<?> aClass, DiscoveryIssueReporter issueReporter) {
        try {
            return Optional.of(new AutoTestResolver().resolveTest(engineDesc, config, aClass));
        } catch (Exception e) {
            issueReporter.reportIssue(DiscoveryIssue.builder(DiscoveryIssue.Severity.ERROR,
                    "Failed to resolve auto-test class: " + aClass.getName()).cause(e).build());
            return Optional.empty();
        }
    }

    /*
    @Override
    public void execute(ExecutionRequest request) {
        TestDescriptor root = request.getRootTestDescriptor();

        new AutomatedTestExecutor().execute(request,root);
    }
     */
}
