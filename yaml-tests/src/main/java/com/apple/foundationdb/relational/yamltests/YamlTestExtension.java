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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
                    new MultiServerConfig(1, 1113, 1114)
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
        return testConfigs.stream().map(config -> new Context(config, context.getRequiredTestMethod().getAnnotation(ExcludeYamlTestConfig.class)));
    }

    private static class Context implements TestTemplateInvocationContext {
        private final YamlTestConfig config;
        private final Set<Class<? extends YamlTestConfig>> excludedConfigs;

        public Context(final YamlTestConfig config, final ExcludeYamlTestConfig excludedConfigs) {
            this.config = config;
            this.excludedConfigs = excludedConfigs == null ? Set.of() : Set.of(excludedConfigs.value());
        }


        @Override
        public String getDisplayName(int invocationIndex) {
            return config.toString();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return List.of(new ClassParameterResolver(config, excludedConfigs));
        }
    }

    private static final class ClassParameterResolver implements ParameterResolver {

        private final YamlTestConfig config;
        private final Set<Class<? extends YamlTestConfig>> excludedConfigs;

        public ClassParameterResolver(YamlTestConfig config, final Set<Class<? extends YamlTestConfig>> excludedConfigs) {
            this.config = config;
            this.excludedConfigs = excludedConfigs;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return parameterContext.getParameter().getType().equals(YamlTest.Runner.class);
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return new YamlTest.Runner() {
                @Override
                public void run(final String fileName) throws Exception {
                    run(fileName, false);
                }

                @Override
                public void run(final String fileName, final boolean correctExplain) throws Exception {
                    config.assumeSupport(fileName, excludedConfigs);
                    Assumptions.assumeFalse(excludedConfigs.contains(config.getClass()));
                    var yamlRunner = new YamlRunner(fileName, config.createConnectionFactory(), correctExplain);
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
