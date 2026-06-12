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

import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that {@link RealVectorPrimitives} resolves to the backend that the current task's JVM
 * setup implies. Each test is routed by a JUnit tag to the gradle task whose backend it needs:
 * <ul>
 *   <li>{@link Tags#RequiresSIMD}: runs in the default {@code test} task, which passes
 *       {@code --add-modules jdk.incubator.vector} so the SIMD backend loads.</li>
 *   <li>{@link Tags#RequiresScalar}: runs in {@code scalarFallbackTest}, which sets
 *       {@code fdb.vector.simd=scalar} and omits {@code --add-modules}.</li>
 * </ul>
 */
class BackendSelectionTest {

    @Test
    @Tag(Tags.RequiresSIMD)
    void simdBackendIsActiveWhenIncubatorModuleAvailable() {
        final String backendName = RealVectorPrimitives.backend().name();
        assertThat(backendName)
                .as("backend name when --add-modules jdk.incubator.vector is set and "
                        + "fdb.vector.simd is unset (or 'auto'/'simd')")
                .startsWith("simd");
    }

    @Test
    @Tag(Tags.RequiresScalar)
    void scalarBackendIsActiveWhenForcedToScalar() {
        final String backendName = RealVectorPrimitives.backend().name();
        assertThat(backendName)
                .as("backend name when fdb.vector.simd=scalar is set")
                .isEqualTo("scalar");
    }
}
