/*
 * EvaluationContextTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@link EvaluationContext} class.
 */
class EvaluationContextTest {
    @Test
    void emptyContext() {
        EvaluationContext evaluationContext = EvaluationContext.empty();
        assertMissingBinding(evaluationContext, "foo");
        assertSame(TypeRepository.EMPTY_SCHEMA, evaluationContext.getTypeRepository());
    }

    @Test
    void withBinding() {
        EvaluationContext evaluationContext = EvaluationContext.forBinding("foo", "bar");
        assertEquals("bar", evaluationContext.getBinding("foo"));
        assertMissingBinding(evaluationContext, "unknown");
        assertSame(TypeRepository.EMPTY_SCHEMA, evaluationContext.getTypeRepository());
    }

    @Test
    void builderPreservesBindings() {
        Bindings bindings = Bindings.newBuilder()
                .set("key_a", "value_a")
                .set("key_b", "value_b")
                .build();
        EvaluationContext evaluationContext = EvaluationContext.forBindings(bindings);
        EvaluationContext newContext = evaluationContext.childBuilder().build();
        assertBindingsMatch(bindings, newContext);
    }

    @Test
    void builderBindingsDoNotAffectParent() {
        Bindings bindings = Bindings.newBuilder()
                .set("a", 'a')
                .set("b", 'b')
                .set("c", 'c')
                .build();
        EvaluationContext evaluationContext = EvaluationContext.forBindings(bindings);
        EvaluationContext newContext = evaluationContext.childBuilder()
                .setBinding("d", 'd')
                .build();
        // New key "d" should be in only the new context, not the original one
        assertEquals('d', newContext.getBinding("d"));
        assertMissingBinding(evaluationContext, "d");
        assertMissingBinding(bindings, "d");
    }

    @Test
    void builderPreservesTypeRepository() {
        TypeRepository typeRepository = TypeRepository.newBuilder()
                .addTypeIfNeeded(Type.primitiveType(Type.TypeCode.BYTES))
                .build();
        EvaluationContext context = EvaluationContext.forTypeRepository(typeRepository);
        assertSame(typeRepository, context.getTypeRepository());
        EvaluationContext newContext = context.childBuilder().build();
        assertSame(typeRepository, newContext.getTypeRepository());
    }

    @Test
    void builderCanChangeTypeRepository() {
        TypeRepository repo1 = TypeRepository.newBuilder()
                .addTypeIfNeeded(Type.primitiveType(Type.TypeCode.BOOLEAN))
                .build();
        TypeRepository repo2 = TypeRepository.newBuilder()
                .addTypeIfNeeded(Type.primitiveType(Type.TypeCode.INT))
                .build();
        Bindings bindings = Bindings.newBuilder()
                .set("foo", 0)
                .set("bar", 1)
                .build();
        EvaluationContext context1 = EvaluationContext.forBindingsAndTypeRepository(bindings, repo1);
        assertSame(repo1, context1.getTypeRepository());
        assertBindingsMatch(bindings, context1);

        EvaluationContext context2 = context1.childBuilder()
                .setTypeRepository(repo2)
                .build();
        assertSame(repo2, context2.getTypeRepository());
        assertBindingsMatch(bindings, context2);
    }

    private static void assertBindingsMatch(Bindings expectedBindings, EvaluationContext evaluationContext) {
        assertThat(evaluationContext.getBindings().asMappingList(),
                containsInAnyOrder(expectedBindings.asMappingList().stream().map(Matchers::equalTo).collect(Collectors.toList())));
    }

    private static void assertMissingBinding(EvaluationContext evaluationContext, String key) {
        RecordCoreException err = assertThrows(RecordCoreException.class, () -> evaluationContext.getBinding(key));
        assertThat(err.getMessage(), containsString(String.format("Missing binding for %s", key)));
    }

    private static void assertMissingBinding(Bindings bindings, String key) {
        RecordCoreException err = assertThrows(RecordCoreException.class, () -> bindings.get(key));
        assertThat(err.getMessage(), containsString(String.format("Missing binding for %s", key)));
    }
}
