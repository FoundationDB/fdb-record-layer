/*
 * QueryEvaluationConstantsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryEvaluationConstantsTest {
    @Test
    void testStream() {
        final var evaluationTimestamp = Instant.now();
        final var evaluationConstants = QueryEvaluationConstants
                .newBuilder()
                .setEvaluationTimestamp(evaluationTimestamp)
                .build();

        final var asList = evaluationConstants.stream()
                .collect(ImmutableList.toImmutableList());

        assertThat(asList).singleElement().satisfies((constant) -> {
            assertThat(constant.getKind()).isEqualTo(
                    QueryEvaluationConstants.QueryEvaluationConstant.ConstantKind.EVALUATION_TIMESTAMP);
            assertThat(constant.getValue()).isEqualTo(evaluationTimestamp.toEpochMilli());
        });
    }

    @Test
    void isEmpty() {
        assertThat(QueryEvaluationConstants.newBuilder().build().isEmpty()).isTrue();
        assertThat(QueryEvaluationConstants.empty().isEmpty()).isTrue();
    }

    @Test
    void asMapByConstantId() {
        final var evaluationTimestamp = Instant.now();
        final var evaluationConstants = QueryEvaluationConstants
                .newBuilder()
                .setEvaluationTimestamp(evaluationTimestamp)
                .build();

        final var asMap = evaluationConstants.asMapByConstantId();

        assertThat(asMap).containsExactly(
                Map.entry(
                        QueryEvaluationConstants.QueryEvaluationConstant.ConstantKind.EVALUATION_TIMESTAMP
                                .getConstantObjectValue().getConstantId(),
                        evaluationTimestamp.toEpochMilli()));
    }

    @Test
    void testQueryEvaluationConstantEquals() {
        final var constant = evaluationTimestampConstant(1_000L);
        final var sameValueConstant = evaluationTimestampConstant(1_000L);
        final var differentValueConstant = evaluationTimestampConstant(2_000L);

        assertThat(constant)
                .isEqualTo(constant)
                .isEqualTo(sameValueConstant)
                .isNotEqualTo(differentValueConstant);
    }

    @Test
    void testQueryEvaluationConstantHashCode() {
        final var constant = evaluationTimestampConstant(
                Instant.ofEpochMilli(1_000L));
        final var sameValueConstant = evaluationTimestampConstant(
                Instant.ofEpochMilli(1_000L));
        final var differentValueConstant = evaluationTimestampConstant(
                Instant.ofEpochMilli(2_000L));

        assertThat(constant)
                .hasSameHashCodeAs(constant)
                .hasSameHashCodeAs(sameValueConstant)
                .doesNotHaveSameHashCodeAs(differentValueConstant);
    }

    @Test
    void testQueryEvaluationConstantToString() {
        final var constant = evaluationTimestampConstant(1_000L);

        assertThat(constant).hasToString("EVALUATION_TIMESTAMP=1000");
    }

    @Nonnull
    private static QueryEvaluationConstants.QueryEvaluationConstant evaluationTimestampConstant(@Nonnull Object timestampValue) {
        return new QueryEvaluationConstants.QueryEvaluationConstant(
                QueryEvaluationConstants.QueryEvaluationConstant.ConstantKind.EVALUATION_TIMESTAMP,
                timestampValue);
    }
}
