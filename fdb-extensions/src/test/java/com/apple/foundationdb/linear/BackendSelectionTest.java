/*
 * BackendSelectionTest.java
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

package com.apple.foundationdb.linear;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that {@link RealVectorPrimitives} resolves to the backend that the current task's JVM
 * setup implies. Each test is gated by the {@code fdb.vector.simd} system property the gradle task
 * sets:
 * <ul>
 *   <li>Default {@code test} task: no system property, JVM has {@code --add-modules
 *       jdk.incubator.vector}. Backend should be SIMD.</li>
 *   <li>{@code testScalarFallback}: {@code fdb.vector.simd=scalar}, no {@code --add-modules}.
 *       Backend should be scalar.</li>
 * </ul>
 */
class BackendSelectionTest {

    @Test
    @DisabledIfSystemProperty(named = "fdb.vector.simd", matches = "(?i)scalar")
    void simdBackendIsActiveWhenIncubatorModuleAvailable() {
        final String backendName = RealVectorPrimitives.backend().name();
        assertThat(backendName)
                .as("backend name when --add-modules jdk.incubator.vector is set and "
                        + "fdb.vector.simd is unset (or 'auto'/'simd')")
                .startsWith("simd");
    }

    @Test
    @EnabledIfSystemProperty(named = "fdb.vector.simd", matches = "scalar")
    void scalarBackendIsActiveWhenForcedToScalar() {
        final String backendName = RealVectorPrimitives.backend().name();
        assertThat(backendName)
                .as("backend name when fdb.vector.simd=scalar is set")
                .isEqualTo("scalar");
    }
}
