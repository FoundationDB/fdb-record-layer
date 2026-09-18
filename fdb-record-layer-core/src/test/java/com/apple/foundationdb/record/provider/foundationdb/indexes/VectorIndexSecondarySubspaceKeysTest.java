/*
 * VectorIndexSecondarySubspaceKeysTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards {@link VectorIndexSecondarySubspaceKeys} against a prefix clash: every {@code static final long} prefix it
 * declares must be distinct, so two kinds of secondary-subspace data can never be written under the same key prefix.
 * Gathering the prefixes in one class already makes a clash obvious on review; this makes it fail CI too.
 */
class VectorIndexSecondarySubspaceKeysTest {
    @Test
    void allPrefixesAreDistinct() throws IllegalAccessException {
        final List<Long> prefixes = new ArrayList<>();
        for (final Field field : VectorIndexSecondarySubspaceKeys.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == long.class) {
                prefixes.add(field.getLong(null));
            }
        }

        assertThat(prefixes).as("VectorIndexSecondarySubspaceKeys must declare its prefixes as static final long")
                .isNotEmpty();
        assertThat(prefixes).as("every secondary-subspace prefix must be distinct so the kinds never collide")
                .doesNotHaveDuplicates();
    }
}
