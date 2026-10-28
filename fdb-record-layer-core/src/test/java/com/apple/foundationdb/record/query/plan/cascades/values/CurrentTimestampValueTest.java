/*
 * CurrentTimestampValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CurrentTimestampValueTest {
    @Test
    void testEquals() {
        final var v1 = CurrentTimestampValue.of(
                ConstantObjectValue.of(Quantifier.constant(),
                        "c1", Type.primitiveType(Type.TypeCode.LONG)));
        final var v2 = CurrentTimestampValue.of(
                ConstantObjectValue.of(Quantifier.constant(),
                        "c2", Type.primitiveType(Type.TypeCode.LONG)));
        final var v3 = CurrentTimestampValue.of(
                ConstantObjectValue.of(Quantifier.constant(),
                        "c1", Type.primitiveType(Type.TypeCode.LONG)));

        assertThat(v1).isEqualTo(v1).isEqualTo(v3).isNotEqualTo(v2);
    }

    @Test
    void testExplain() {
        final var v1 = CurrentTimestampValue.of(
                ConstantObjectValue.of(Quantifier.constant(),
                        "c1", Type.primitiveType(Type.TypeCode.LONG)));

        assertThat(v1.explain().getExplainTokens().render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)))
                .hasToString("current_timestamp(@c1)");
    }

    @Test
    void testEvalReturnsChildTimestampValue() {
        final var expectedTimestamp = 10L;
        final var v1 = CurrentTimestampValue.of(
                ConstantObjectValue.of(Quantifier.constant(),
                        "c1", Type.primitiveType(Type.TypeCode.LONG)));
        final var evaluationContext = EvaluationContext.newBuilder()
                .setConstant(Quantifier.constant(), Map.of("c1", expectedTimestamp))
                .build(TypeRepository.empty());

        assertThat(v1.evalWithoutStore(evaluationContext))
                .isEqualTo(expectedTimestamp);
    }
}
