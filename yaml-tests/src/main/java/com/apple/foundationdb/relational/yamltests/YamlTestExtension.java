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

import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
import com.apple.foundationdb.relational.yamltests.configs.ForceContinuations;
import com.apple.foundationdb.relational.yamltests.configs.JDBCInProcessConfig;
import com.apple.foundationdb.relational.yamltests.configs.MultiServerConfig;
import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Extension that backs {@link YamlTest}.
 */
public class YamlTestExtension implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {
    private static final Logger logger = LogManager.getLogger(YamlTestExtension.class);
    private List<YamlTestConfig> testConfigs;

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        if (Boolean.parseBoolean(System.getProperty("tests.runQuick", "false"))) {
            testConfigs = List.of(new EmbeddedConfig());
        } else {
            testConfigs = List.of(
                    new EmbeddedConfig(),
                    new JDBCInProcessConfig(),
                    new MultiServerConfig(0, 1111, 1112),
                    new ForceContinuations(new MultiServerConfig(0, 1113, 1114)),
                    new MultiServerConfig(1, 1115, 1116),
                    new ForceContinuations(new MultiServerConfig(1, 1117, 1118))
            );
        }
        for (final YamlTestConfig testConfig : testConfigs) {
            testConfig.beforeAll();
        }
    }

    @Override
    @SuppressWarnings("PMD.UnnecessaryLocalBeforeReturn") // It complains about the local variable `e` being returned
    public void afterAll(final ExtensionContext context) throws Exception {
        final Optional<Exception> exception = testConfigs.stream().map(config -> {
            try {
                config.afterAll();
                return null;
            } catch (Exception e) {
                return e;
            }
        }).filter(Objects::nonNull).findFirst();
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
        // excluded tests are still included as configs so that they show up in the test run as skipped, rather than
        // just not being there. This may waste some resources if all the tests being run exclude a config that has
        // expensive @BeforeAll.
        return testConfigs.stream().map(config -> new Context(config, context.getRequiredTestMethod().getAnnotation(ExcludeYamlTestConfig.class)));
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
        private final YamlTestConfigExclusions exclusion;

        public Context(@Nonnull final YamlTestConfig config, @Nullable final ExcludeYamlTestConfig excludedConfigs) {
            this.config = config;
            this.exclusion = excludedConfigs == null ? null : excludedConfigs.value();
            this.excludedReason = excludedConfigs == null ? "" : excludedConfigs.reason();
        }


        @Override
        public String getDisplayName(int invocationIndex) {
            return config.toString();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return List.of(new ClassParameterResolver(config, exclusion, excludedReason));
        }
    }

    /**
     * Parameter resolver for the class when injecting the {@link YamlTest.Runner}.
     */
    private static final class ClassParameterResolver implements ParameterResolver {

        @Nonnull
        private final YamlTestConfig config;
        @Nullable
        private final YamlTestConfigExclusions exclusion;
        @Nullable
        private final String excludedReason;

        public ClassParameterResolver(@Nonnull final YamlTestConfig config,
                                      @Nullable final YamlTestConfigExclusions exclusion,
                                      @Nullable final String excludedReason) {
            this.config = config;
            this.exclusion = exclusion;
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
                    runYamsql(fileName, EnumSet.noneOf(YamlRunner.YamlRunnerOptions.class));
                }

                @Override
                public void runYamsql(final String fileName, final EnumSet<YamlRunner.YamlRunnerOptions> yamlRunnerOptions) throws Exception {
                    if (exclusion != null) {
                        Assumptions.assumeFalse(exclusion.check(config), excludedReason);
                    }
                    var yamlRunner = new YamlRunner(fileName, config.createConnectionFactory(), yamlRunnerOptions,
                            config.getRunnerOptions());
                    try {
                        yamlRunner.run();
                    } catch (Exception e) {
                        logger.error("‼️ running test file '{}' was not successful", fileName, e);
                        throw e;
                    }
                }
            };
        }
    }
}
