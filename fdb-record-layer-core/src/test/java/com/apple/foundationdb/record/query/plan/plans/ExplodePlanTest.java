/*
 * ExplodePlanTest.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class ExplodePlanTest {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static final class ExplodeCursorBuilder {

        @Nonnull
        private final RecordQueryPlan explodePlan;

        @Nonnull
        private Optional<Integer> skip;

        @Nonnull
        private Optional<Integer> limit;

        private ExplodeCursorBuilder() {
            explodePlan = generateExplodePlan();
            skip = Optional.empty();
            limit = Optional.empty();
        }

        @Nonnull
        ExplodeCursorBuilder withSkip(int skip) {
            this.skip = Optional.of(skip);
            return this;
        }

        @Nonnull
        ExplodeCursorBuilder withLimit(int limit) {
            this.limit = Optional.of(limit);
            return this;
        }

        @Override
        public String toString() {
            return "explode [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] "
                    + limit.map(l -> "limit " + l + " ").orElse("")
                    + skip.map(s -> "skip " + s).orElse("");
        }

        @Nonnull
        @SuppressWarnings("DataFlowIssue") // explode transposes the underlying constant array Value, it does not strictly require a record store instance.
        RecordCursor<QueryResult> build() {
            final var executionPropertiesBuilder = ExecuteProperties.newBuilder();
            skip.ifPresent(executionPropertiesBuilder::setSkip);
            limit.ifPresent(executionPropertiesBuilder::setReturnedRowLimit);
            final var executionProperties = executionPropertiesBuilder.build();
            return explodePlan.executePlan(null, EvaluationContext.EMPTY, null, executionProperties);
        }

        @Nonnull
        private static RecordQueryPlan generateExplodePlan() {
            final Value collectionValue = LiteralValue.ofList(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
            return new RecordQueryExplodePlan(collectionValue);
        }

        @Nonnull
        public static ExplodeCursorBuilder instance() {
            return new ExplodeCursorBuilder();
        }
    }

    private static void verifyCursor(@Nonnull final RecordCursor<QueryResult> actualCursor,
                                     @Nonnull final List<Integer> expectedResults,
                                     boolean verifyLimitExceeded) {
        for (final var expectedValue : expectedResults) {
            final var result = actualCursor.getNext();
            Assertions.assertTrue(result.hasNext());
            Assertions.assertNotNull(result.get());
            Assertions.assertEquals(expectedValue, Objects.requireNonNull(result.get()).getDatum());
        }
        if (verifyLimitExceeded) {
            final var result = actualCursor.getNext();
            Assertions.assertFalse(result.hasNext());
            Assertions.assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());
        }
    }

    private static class ArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(1), ImmutableList.of(1), true),
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(4), ImmutableList.of(1, 2, 3, 4), true),
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(1).withSkip(3), ImmutableList.of(4), true),
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(2).withSkip(5), ImmutableList.of(6, 7), true),
                    Arguments.of(ExplodeCursorBuilder.instance(), ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), false));
        }
    }

    @ParameterizedTest(name = "{0} should return {1}")
    @ArgumentsSource(ArgumentProvider.class)
    void explodeWithSkipAndLimitWorks(@Nonnull final ExplodeCursorBuilder actualCursorBuilder,
                                      @Nonnull final List<Integer> expectedResult,
                                      boolean shouldReachLimit) {
        verifyCursor(actualCursorBuilder.build(), expectedResult, shouldReachLimit);
    }
}
