/*
 * ColumnarValueTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ColumnarValue}. No FDB connection required.
 */
public class ColumnarValueTest {

    // ── construction ─────────────────────────────────────────────────────────

    @Test
    void getColumnIdReturnsConstructedValue() {
        final var inner = new LiteralValue<>(42);
        assertEquals(7, new ColumnarValue(inner, 7).getColumnId());
    }

    @Test
    void getInnerReturnsConstructedValue() {
        final var inner = new LiteralValue<>(42);
        assertSame(inner, new ColumnarValue(inner, 0).getInner());
    }

    // ── delegation ───────────────────────────────────────────────────────────

    @Test
    void getResultTypeDelegatesToInner() {
        final var inner = new LiteralValue<>(42);
        assertEquals(inner.getResultType(), new ColumnarValue(inner, 0).getResultType());
    }

    @Test
    void evalDelegatesToInner() {
        final var inner = new LiteralValue<>("hello");
        assertEquals("hello", new ColumnarValue(inner, 0).eval(null, EvaluationContext.empty()));
    }

    @Test
    void isIndexOnlyDelegatesToInner() {
        final var inner = new LiteralValue<>(1);
        assertEquals(inner.isIndexOnly(), new ColumnarValue(inner, 0).isIndexOnly());
    }

    @Test
    void getChildrenDelegatesToInner() {
        final var inner = new LiteralValue<>(1);
        assertEquals(inner.getChildren(), new ColumnarValue(inner, 0).getChildren());
    }

    @Test
    void semanticHashCodeDelegatesToInner() {
        final var inner = new LiteralValue<>(99);
        assertEquals(inner.semanticHashCode(), new ColumnarValue(inner, 3).semanticHashCode());
    }

    @Test
    void hashCodeWithoutChildrenDelegatesToInner() {
        final var inner = new LiteralValue<>(99);
        assertEquals(inner.hashCodeWithoutChildren(), new ColumnarValue(inner, 3).hashCodeWithoutChildren());
    }

    // ── withChildren ─────────────────────────────────────────────────────────

    @Test
    void withChildrenPreservesColumnId() {
        final var inner = new LiteralValue<>(1);
        final var original = new ColumnarValue(inner, 5);
        final var rebuilt = (ColumnarValue) original.withChildren(List.of());
        assertEquals(5, rebuilt.getColumnId());
        assertSame(inner, rebuilt.getInner());
    }

    // ── semanticEquals ───────────────────────────────────────────────────────

    @Test
    void semanticEqualsIgnoresColumnId() {
        final var inner = new LiteralValue<>(42);
        final var cv0 = new ColumnarValue(inner, 0);
        final var cv5 = new ColumnarValue(inner, 5);
        assertTrue(cv0.semanticEquals(cv5, AliasMap.emptyMap()));
    }

    @Test
    void semanticEqualsReturnsFalseForDifferentInner() {
        final var cv1 = new ColumnarValue(new LiteralValue<>(1), 0);
        final var cv2 = new ColumnarValue(new LiteralValue<>(2), 0);
        assertFalse(cv1.semanticEquals(cv2, AliasMap.emptyMap()));
    }

    @Test
    void semanticEqualsWithNonColumnarValueDelegatesToInner() {
        final var inner = new LiteralValue<>(42);
        final var cv = new ColumnarValue(inner, 0);
        // When other is not a ColumnarValue, delegates to inner.semanticEquals(other, aliasMap).
        assertEquals(inner.semanticEquals(inner, AliasMap.emptyMap()),
                cv.semanticEquals(inner, AliasMap.emptyMap()));
    }

    // ── untested-gap coverage ─────────────────────────────────────────────────

    @Test
    void getCorrelatedToWithoutChildrenDelegatesToInner() {
        final var inner = new LiteralValue<>(1);
        assertEquals(inner.getCorrelatedToWithoutChildren(),
                new ColumnarValue(inner, 0).getCorrelatedToWithoutChildren());
    }

    @Test
    void planHashDelegatesToInner() {
        final var inner = new LiteralValue<>(42);
        final var cv = new ColumnarValue(inner, 3);
        assertEquals(inner.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                cv.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertEquals(inner.planHash(PlanHashable.CURRENT_LEGACY),
                cv.planHash(PlanHashable.CURRENT_LEGACY));
    }

    @Test
    void explainDelegatesToInner() {
        final var inner = new LiteralValue<>(42);
        final var cv = new ColumnarValue(inner, 0);
        final var formatter = DefaultExplainFormatter.forDebugging();
        assertEquals(inner.explain(List.of()).getExplainTokens().render(formatter).toString(),
                cv.explain(List.of()).getExplainTokens().render(formatter).toString());
    }

    @Test
    void toProtoDelegatesToInner() {
        final var inner = new LiteralValue<>(42);
        final var cv = new ColumnarValue(inner, 0);
        final var serCtx = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        assertEquals(inner.toProto(serCtx), cv.toProto(serCtx));
    }

    @Test
    void toValueProtoDelegatesToInner() {
        final var inner = new LiteralValue<>(42);
        final var cv = new ColumnarValue(inner, 0);
        final var serCtx = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        assertEquals(inner.toValueProto(serCtx), cv.toValueProto(serCtx));
    }
}
