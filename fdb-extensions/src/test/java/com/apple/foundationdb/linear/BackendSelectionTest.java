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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RealVectorPrimitives} backend selection, in two flavors.
 * <p>
 * <b>Active-backend assertions</b> check that the statically resolved {@link RealVectorPrimitives#backend()}
 * matches what the current task's JVM setup implies. Each is routed by a JUnit tag to the gradle
 * task whose backend it needs:
 * <ul>
 *   <li>{@link Tags#RequiresSIMD}: runs in the default {@code test} task, which passes
 *       {@code --add-modules jdk.incubator.vector} so the SIMD backend loads.</li>
 *   <li>{@link Tags#RequiresScalar}: runs in {@code scalarFallbackTest}, which sets
 *       {@code fdb.vector.simd=scalar} and omits {@code --add-modules}.</li>
 * </ul>
 * <p>
 * <b>Selection-logic tests</b> drive {@link RealVectorPrimitives#selectBackend(String,
 * RealVectorPrimitives.SimdBackendLoader)} directly with a stub loader, so every branch (auto,
 * scalar, strict simd; loader success/failure) is covered deterministically in the default JVM
 * regardless of module availability. Two {@link Tags#RequiresScalar}-tagged variants additionally
 * use the <em>real</em> reflective loader in the no-{@code --add-modules} fork to verify the
 * genuine module-absent behavior.
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

    // ---- selection-logic unit tests (stub loader; run in any JVM) ----

    @Test
    void autoModeUsesSimdBackendWhenLoaderSucceeds() {
        final Backend stub = new ScalarBackend(); // sentinel; any Backend instance works here
        assertThat(RealVectorPrimitives.selectBackend("auto", () -> stub)).isSameAs(stub);
    }

    @Test
    void autoModeFallsBackToScalarWhenLoaderFails() {
        // Default 'auto' mode with the SIMD backend unloadable (e.g. no --add-modules) must
        // silently fall back to scalar — the configuration an adopter gets without opting in.
        final Backend backend = RealVectorPrimitives.selectBackend("auto",
                () -> {
                    throw new NoClassDefFoundError("jdk.incubator.vector not present");
                });
        assertThat(backend).isInstanceOf(ScalarBackend.class);
    }

    @Test
    void scalarModeUsesScalarWithoutConsultingLoader() {
        final Backend backend = RealVectorPrimitives.selectBackend("scalar",
                () -> {
                    throw new AssertionError("loader must not be consulted in scalar mode");
                });
        assertThat(backend).isInstanceOf(ScalarBackend.class);
    }

    @Test
    void simdModeUsesSimdBackendWhenLoaderSucceeds() {
        final Backend stub = new ScalarBackend();
        assertThat(RealVectorPrimitives.selectBackend("simd", () -> stub)).isSameAs(stub);
    }

    @Test
    void simdModeThrowsWhenLoaderFails() {
        // Strict opt-in: -Dfdb.vector.simd=simd must fail loudly rather than silently degrade.
        assertThatThrownBy(() -> RealVectorPrimitives.selectBackend("simd",
                () -> {
                    throw new NoClassDefFoundError("jdk.incubator.vector not present");
                }))
                .isInstanceOf(IllegalStateException.class);
    }

    // ---- module-absent integration tests (real reflective loader; scalarFallbackTest fork only) ----

    @Test
    @Tag(Tags.RequiresScalar)
    void autoModeFallsBackToScalarWithRealLoaderWhenModuleAbsent() {
        // In the scalarFallbackTest fork (no --add-modules) the real reflective load genuinely
        // fails, so 'auto' must fall back to scalar — the real module-absent adopter default.
        final Backend backend =
                RealVectorPrimitives.selectBackend("auto", RealVectorPrimitives::loadSimdBackend);
        assertThat(backend).isInstanceOf(ScalarBackend.class);
    }

    @Test
    @Tag(Tags.RequiresScalar)
    void simdModeThrowsWithRealLoaderWhenModuleAbsent() {
        assertThatThrownBy(() ->
                RealVectorPrimitives.selectBackend("simd", RealVectorPrimitives::loadSimdBackend))
                .isInstanceOf(IllegalStateException.class);
    }
}
