/*
 * RecordLayerViewTests.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.query.plan.cascades.RawView;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RecordLayerView}.
 */
public class RecordLayerViewTests {

    @Nonnull
    private static Function<Boolean, LogicalOperator> createMockCompiler() {
        return (Boolean parameter) -> null;
    }

    @Test
    void testBasicBuilder() {
        final RecordLayerView view = RecordLayerView.newBuilder()
                .setName("test_view")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        assertThat(view.getName()).isEqualTo("test_view");
        assertThat(view.getDescription()).isEqualTo("SELECT * FROM table");
        assertThat(view.isTemporary()).isFalse();
        assertThat(view.getCompilableViewSupplier()).isNotNull();
    }

    @Test
    void testTemporaryView() {
        final RecordLayerView view = RecordLayerView.newBuilder()
                .setName("temp_view")
                .setDescription("SELECT id FROM employees")
                .setTemporary(true)
                .setViewCompiler(createMockCompiler())
                .build();

        assertThat(view.isTemporary()).isTrue();
    }

    @Test
    void testBuilderRequiresName() {
        Assertions.assertThrows(Exception.class, () ->
                RecordLayerView.newBuilder()
                        .setDescription("SELECT * FROM table")
                        .setTemporary(false)
                        .setViewCompiler(createMockCompiler())
                        .build()
        );
    }

    @Test
    void testBuilderRequiresDescription() {
        Assertions.assertThrows(Exception.class, () ->
                RecordLayerView.newBuilder()
                        .setName("test_view")
                        .setTemporary(false)
                        .setViewCompiler(createMockCompiler())
                        .build()
        );
    }

    @Test
    void testBuilderRequiresViewCompiler() {
        Assertions.assertThrows(Exception.class, () ->
                RecordLayerView.newBuilder()
                        .setName("test_view")
                        .setDescription("SELECT * FROM table")
                        .setTemporary(false)
                        .build()
        );
    }

    @Test
    void testEqualityWithSameValues() {
        final Function<Boolean, LogicalOperator> compiler = createMockCompiler();

        final RecordLayerView view1 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(compiler)
                .build();

        final RecordLayerView view2 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(compiler)
                .build();

        assertThat(view1).isEqualTo(view2);
        assertThat(view1.hashCode()).isEqualTo(view2.hashCode());
    }

    @Test
    void testEqualityIgnoresViewCompiler() {
        final RecordLayerView view1 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        final RecordLayerView view2 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        assertThat(view1).isEqualTo(view2);
        assertThat(view1.hashCode()).isEqualTo(view2.hashCode());
    }

    @Test
    void testInequalityWithDifferentNames() {
        final Function<Boolean, LogicalOperator> compiler = createMockCompiler();

        final RecordLayerView view1 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(compiler)
                .build();

        final RecordLayerView view2 = RecordLayerView.newBuilder()
                .setName("view2")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(compiler)
                .build();

        assertThat(view1).isNotEqualTo(view2);
    }

    @Test
    void testInequalityWithDifferentDescriptions() {
        final Function<Boolean, LogicalOperator> compiler = createMockCompiler();

        final RecordLayerView view1 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table1")
                .setTemporary(false)
                .setViewCompiler(compiler)
                .build();

        final RecordLayerView view2 = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table2")
                .setTemporary(false)
                .setViewCompiler(compiler)
                .build();

        assertThat(view1).isNotEqualTo(view2);
    }

    @Test
    void testInequalityWithNull() {
        final RecordLayerView view = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        assertThat(view).isNotEqualTo(null);
    }

    @Test
    void testHashCodeConsistency() {
        final RecordLayerView view = RecordLayerView.newBuilder()
                .setName("view1")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        final int hash1 = view.hashCode();
        final int hash2 = view.hashCode();

        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void testToBuilder() {
        final Function<Boolean, LogicalOperator> compiler = createMockCompiler();

        final RecordLayerView original = RecordLayerView.newBuilder()
                .setName("original_view")
                .setDescription("SELECT * FROM table")
                .setTemporary(true)
                .setViewCompiler(compiler)
                .build();

        final RecordLayerView copy = original.toBuilder().build();

        assertThat(copy.getName()).isEqualTo(original.getName());
        assertThat(copy.getDescription()).isEqualTo(original.getDescription());
        assertThat(copy.isTemporary()).isEqualTo(original.isTemporary());
        assertThat(copy).isEqualTo(original);
    }

    @Test
    void testToBuilderWithModifications() {
        final RecordLayerView original = RecordLayerView.newBuilder()
                .setName("original_view")
                .setDescription("SELECT * FROM table")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        final RecordLayerView modified = original.toBuilder()
                .setName("modified_view")
                .setDescription("SELECT id FROM table")
                .setTemporary(true)
                .build();

        assertThat(modified.getName()).isEqualTo("modified_view");
        assertThat(modified.getDescription()).isEqualTo("SELECT id FROM table");
        assertThat(modified.isTemporary()).isTrue();
        assertThat(modified).isNotEqualTo(original);
    }

    @Test
    void testAsRawView() {
        final RecordLayerView view = RecordLayerView.newBuilder()
                .setName("test_view")
                .setDescription("SELECT * FROM employees WHERE salary > 50000")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        final RawView rawView = view.asRawView();

        assertThat(rawView.getName()).isEqualTo("test_view");
        assertThat(rawView.getDefinition()).isEqualTo("SELECT * FROM employees WHERE salary > 50000");
    }

    @Test
    void testToString() {
        final RecordLayerView view = RecordLayerView.newBuilder()
                .setName("my_view")
                .setDescription("SELECT id, name FROM users")
                .setTemporary(false)
                .setViewCompiler(createMockCompiler())
                .build();

        final String toString = view.toString();

        assertThat(toString).contains("my_view");
        assertThat(toString).contains("SELECT id, name FROM users");
    }
}
