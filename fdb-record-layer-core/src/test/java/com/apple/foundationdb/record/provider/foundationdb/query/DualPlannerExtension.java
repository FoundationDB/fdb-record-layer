/*
 * DualPlannerExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.RecordCoreException;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.platform.commons.util.AnnotationUtils;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A JUnit 5 extension that runs the test (which must inherit from {@link FDBRecordStoreQueryTestBase}) with both the
 * old {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} and the new, experimental
 * {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
 */
public class DualPlannerExtension implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return AnnotationUtils.isAnnotated(context.getTestMethod(), DualPlannerTest.class) &&
               FDBRecordStoreQueryTestBase.class.isAssignableFrom(context.getRequiredTestClass());
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        final String displayName = context.getDisplayName();
        if (AnnotationUtils.isAnnotated(context.getTestMethod(), ParameterizedTest.class)) {
            TestTemplateInvocationContextProvider nestedProvider;
            try {
                Constructor<?> nestedProviderConstructor =
                        Class.forName("org.junit.jupiter.params.ParameterizedTestExtension").getDeclaredConstructor();
                nestedProviderConstructor.setAccessible(true);
                nestedProvider = (TestTemplateInvocationContextProvider) nestedProviderConstructor.newInstance();
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RecordCoreException(e.getClass() + " " + e.getMessage());
            }
            return nestedProvider.provideTestTemplateInvocationContexts(context).map(existingContext ->
                            new DualPlannerTestInvocationDisplayNameDecorator(existingContext, true)); // new planner
        } else {
            final Optional<DualPlannerTest> annotationOptional =
                    AnnotationUtils.findAnnotation(context.getTestMethod(), DualPlannerTest.class);
            if (annotationOptional.isEmpty()) {
                throw new RecordCoreException("dual planner test annotation not found");
            }
            final DualPlannerTest annotation = annotationOptional.get();

            switch (annotation.planner()) {
                case OLD:
                    return Stream.of(
                            new DualPlannerTestInvocationContext(displayName, false)); // old planner
                case CASCADES:
                    return Stream.of(
                            new DualPlannerTestInvocationContext(displayName, true)); // cascades planner
                case BOTH:
                default:
                    return Stream.of(
                            new DualPlannerTestInvocationContext(displayName, false), // old planner
                            new DualPlannerTestInvocationContext(displayName, true)); // cascades planner

            }
        }

    }

    private static class DualPlannerTestInvocationContext implements TestTemplateInvocationContext {
        private final String displayName;
        private final List<Extension> extensions;

        public DualPlannerTestInvocationContext(String testName, boolean useRewritePlanner) {
            this(testName, useRewritePlanner, Collections.emptyList());
        }

        public DualPlannerTestInvocationContext(String baseName, boolean useRewritePlanner, List<Extension> extensions) {
            this.displayName = String.format("%s[%s]", baseName, useRewritePlanner ? "cascades" : "old");
            this.extensions = new ArrayList<>(extensions);
            this.extensions.add((TestInstancePostProcessor) (testInstance, context) ->
                    ((FDBRecordStoreQueryTestBase) testInstance).setUseRewritePlanner(useRewritePlanner));
        }

        @Override
        public String getDisplayName(final int invocationIndex) {
            return displayName;
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return extensions;
        }
    }

    private static class DualPlannerTestInvocationDisplayNameDecorator implements TestTemplateInvocationContext {

        @Nonnull
        private final TestTemplateInvocationContext underlying;

        private final boolean useCascades;

        public DualPlannerTestInvocationDisplayNameDecorator(@Nonnull final TestTemplateInvocationContext underlying, final boolean useCascades) {
            this.underlying = underlying;
            this.useCascades = useCascades;
        }

        @Override
        public String getDisplayName(final int invocationIndex) {
            return underlying.getDisplayName(invocationIndex) + (useCascades ? "[cascades]" : "[heuristics]");
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return underlying.getAdditionalExtensions();
        }
    }
}
