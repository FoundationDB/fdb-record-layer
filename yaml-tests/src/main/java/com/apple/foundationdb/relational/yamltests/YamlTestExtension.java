/*
 * YamlTestExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.yamltests.configs.CorrectExplains;
import com.apple.foundationdb.relational.yamltests.configs.CorrectExplainsAndMetrics;
import com.apple.foundationdb.relational.yamltests.configs.CorrectMetrics;
import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
import com.apple.foundationdb.relational.yamltests.configs.ExternalMultiServerConfig;
import com.apple.foundationdb.relational.yamltests.configs.ForceContinuations;
import com.apple.foundationdb.relational.yamltests.configs.JDBCInProcessConfig;
import com.apple.foundationdb.relational.yamltests.configs.JDBCMultiServerConfig;
import com.apple.foundationdb.relational.yamltests.configs.ShowPlanOnDiff;
import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;
import com.google.common.collect.Iterables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.opentest4j.TestAbortedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extension that backs {@link YamlTest}.
 */
public class YamlTestExtension implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {
    private static final Logger logger = LogManager.getLogger(YamlTestExtension.class);
    private List<YamlTestConfig> testConfigs;
    private List<YamlTestConfig> maintainConfigs;
    private List<ExternalServer> servers;
    @Nullable
    private final String clusterFile;
    private final boolean includeMethodInDescriptions;

    public YamlTestExtension() {
        this(null, false);
    }

    /**
     * Create a new extension with some configuration.
     * @param clusterFile a custom cluster file to use, or {@code null} to inherit it from the environment, namely
     * {@code FDB_CLUSTER_FILE}.
     * @param includeMethodInDescriptions Set this to {@code true} if publishing test results to something that cannot
     * handle complex test hierarchies. In the record layer we maintain the full hierarchy in the output, so this is not
     * necessary, but if integrating some other tools this might be necessary.
     */
    public YamlTestExtension(@Nullable final String clusterFile, final boolean includeMethodInDescriptions) {
        this.clusterFile = clusterFile;
        this.includeMethodInDescriptions = includeMethodInDescriptions;
    }

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        maintainConfigs = List.of(
                new CorrectExplains(new EmbeddedConfig(clusterFile)),
                new CorrectMetrics(new EmbeddedConfig(clusterFile)),
                new CorrectExplainsAndMetrics(new EmbeddedConfig(clusterFile)),
                new ShowPlanOnDiff(new EmbeddedConfig(clusterFile))
        );
        if (Boolean.parseBoolean(System.getProperty("tests.runQuick", "false"))) {
            testConfigs = List.of(new EmbeddedConfig(clusterFile));
        } else if (Boolean.parseBoolean(System.getProperty("tests.runRPC", "false"))) {
            testConfigs = List.of(new JDBCInProcessConfig(clusterFile));
        } else {
            List<File> jars = ExternalServer.getAvailableServers();
            // Fail the test if there are no available servers. This would force the execution in "runQuick" mode in case
            // we don't have access to the artifacts.
            // Potentially, we can relax this a little if all tests are disabled for multi-server execution, but this is
            // not a likely scenario.
            Assertions.assertFalse(jars.isEmpty(), "There are no external servers available to run");
            servers = new ArrayList<>();
            for (File jar : jars) {
                servers.add(new ExternalServer(jar, clusterFile));
            }
            for (ExternalServer server : servers) {
                server.start();
            }
            final boolean mixedModeOnly = Boolean.parseBoolean(System.getProperty("tests.mixedModeOnly", "false"));
            final boolean singleExternalVersionOnly = Boolean.parseBoolean(System.getProperty("tests.singleVersion", "false"));
            Stream<YamlTestConfig> localTestingConfigs = localConfigs(mixedModeOnly, singleExternalVersionOnly);
            Stream<YamlTestConfig> externalServerConfigs = externalServerConfigs(singleExternalVersionOnly);

            testConfigs = Stream.concat(
                    // The configs for local testing (single server)
                    localTestingConfigs,
                    // The configs for multi-server testing
                    externalServerConfigs).collect(Collectors.toList());
        }
        for (final YamlTestConfig testConfig : Iterables.concat(testConfigs, maintainConfigs)) {
            testConfig.beforeAll();
        }
    }

    private Stream<YamlTestConfig> localConfigs(final boolean mixedModeOnly, final boolean singleExternalVersionOnly) {
        if (mixedModeOnly || singleExternalVersionOnly) {
            return Stream.of();
        } else {
            return Stream.of(new EmbeddedConfig(clusterFile), new JDBCInProcessConfig(clusterFile));
        }
    }

    private Stream<YamlTestConfig> externalServerConfigs(final boolean singleExternalVersionOnly) {
        if (singleExternalVersionOnly) {
            return servers.stream()
                    // Create an ExternalServer config with two servers of the same version for each server
                    // (with and without forced continuations)
                    .flatMap(server ->
                            Stream.of(new ExternalMultiServerConfig(0, server, server),
                                    new ForceContinuations(new ExternalMultiServerConfig(0, server, server))));
        } else {
            return servers.stream().flatMap(server ->
                    // (4 configs for each server available)
                    Stream.of(new JDBCMultiServerConfig(0, server, clusterFile),
                            new ForceContinuations(new JDBCMultiServerConfig(0, server, clusterFile)),
                            new JDBCMultiServerConfig(1, server, clusterFile),
                            new ForceContinuations(new JDBCMultiServerConfig(1, server, clusterFile))));
        }
    }

    @Override
    @SuppressWarnings("PMD.UnnecessaryLocalBeforeReturn") // It complains about the local variable `e` being returned
    public void afterAll(final ExtensionContext context) throws Exception {
        final Optional<Exception> exception =
                Stream.concat(
                        // if beforeAll fails in certain places, or isn't run, testConfigs and/or maintainConfigs could
                        // be null
                        Objects.requireNonNullElse(testConfigs, List.<YamlTestConfig>of()).stream(),
                        Objects.requireNonNullElse(maintainConfigs, List.<YamlTestConfig>of()).stream())
                        .map(config -> {
                            try {
                                config.afterAll();
                                return null;
                            } catch (Exception e) {
                                return e;
                            }
                        }).filter(Objects::nonNull).findFirst();
        if (servers != null) {
            for (ExternalServer server : servers) {
                try {
                    server.stop();
                } catch (Exception ex) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to stop server " + server.getVersion() + " on " + server.getPort());
                    }
                }
            }
        }
        if (exception.isPresent()) {
            throw exception.get();
        }
    }

    @Override
    public boolean supportsTestTemplate(final ExtensionContext context) {
        return true; // TODO check type
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
        final var testClass = context.getRequiredTestClass();
        final var testMethod = context.getRequiredTestMethod();
        if (testClass.getAnnotation(MaintainYamlTestConfig.class) != null) {
            final var annotation = testClass.getAnnotation(MaintainYamlTestConfig.class);
            return provideInvocationContextsForMaintenance(annotation, testMethod.getName());
        }
        if (testMethod.getAnnotation(ExcludeYamlTestConfig.class) != null) {
            // excluded tests are still included as configs so that they show up in the test run as skipped, rather than
            // just not being there. This may waste some resources if all the tests being run exclude a config that has
            // expensive @BeforeAll.
            final var annotation = testMethod.getAnnotation(ExcludeYamlTestConfig.class);
            return testConfigs
                    .stream()
                    .map(config -> new Context(config, annotation.reason(), annotation.value(),
                            includeMethodInDescriptions, testMethod.getName()));
        } else if (testMethod.getAnnotation(MaintainYamlTestConfig.class) != null) {
            final var annotation =
                    testMethod.getAnnotation(MaintainYamlTestConfig.class);
            return provideInvocationContextsForMaintenance(annotation, testMethod.getName());
        }
        return testConfigs
                .stream()
                .map(config -> new Context(config, "", null, includeMethodInDescriptions, testMethod.getName()));
    }

    private Stream<TestTemplateInvocationContext> provideInvocationContextsForMaintenance(
            @Nonnull final MaintainYamlTestConfig annotation, @Nonnull final String methodName) {
        return maintainConfigs
                .stream()
                .map(config -> new Context(config, "maintenance not needed",
                        Objects.requireNonNull(annotation.value()), includeMethodInDescriptions, methodName));
    }

    /**
     * Context for an individual test run (a specific pair of a {@link org.junit.jupiter.api.TestTemplate}
     * method and a {@link YamlTestConfig}).
     */
    private static class Context implements TestTemplateInvocationContext {
        @Nonnull
        private final YamlTestConfig config;
        @Nonnull
        private final String excludedReason;
        @Nullable
        private final YamlTestConfigFilters configFilters;
        private final boolean includeMethodInDescriptions;
        @Nonnull
        private final String methodName;

        public Context(@Nonnull final YamlTestConfig config, @Nonnull final String excludedReason,
                       @Nullable final YamlTestConfigFilters configFilters,
                       final boolean includeMethodInDescriptions, @Nonnull final String methodName) {
            this.config = config;
            this.excludedReason = excludedReason;
            this.configFilters = configFilters;
            this.includeMethodInDescriptions = includeMethodInDescriptions;
            this.methodName = methodName;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            if (includeMethodInDescriptions) {
                return methodName + "(" + config + ")";
            } else {
                return config.toString();
            }
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return List.of(new ClassParameterResolver(config, configFilters, excludedReason));
        }
    }

    /**
     * Parameter resolver for the class when injecting the {@link YamlTest.Runner}.
     */
    private static final class ClassParameterResolver implements ParameterResolver {

        @Nonnull
        private final YamlTestConfig config;
        @Nullable
        private final YamlTestConfigFilters filters;
        @Nullable
        private final String excludedReason;

        public ClassParameterResolver(@Nonnull final YamlTestConfig config,
                                      @Nullable final YamlTestConfigFilters filters,
                                      @Nullable final String excludedReason) {
            this.config = config;
            this.filters = filters;
            this.excludedReason = excludedReason;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return parameterContext.getParameter().getType().equals(YamlTest.Runner.class);
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return new YamlTest.Runner() {
                @Override
                public void runYamsql(final String fileName) throws Exception {
                    if (filters != null) {
                        Assumptions.assumeTrue(filters.filter(config), excludedReason);
                    }
                    var yamlRunner = new YamlRunner(fileName, config.createConnectionFactory(),
                            config.getRunnerOptions());
                    try {
                        yamlRunner.run();
                    } catch (TestAbortedException tAE) {
                        throw tAE;
                    } catch (Exception e) {
                        logger.error("‼️ running test file '{}' was not successful", fileName, e);
                        throw e;
                    }
                }
            };
        }
    }
}
