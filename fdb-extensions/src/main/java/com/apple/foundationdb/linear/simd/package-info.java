/*
 * package-info.java
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

/**
 * SIMD-using implementations of vector primitives, depending on the {@code jdk.incubator.vector}
 * incubator module.
 * <p>
 * <b>Isolation invariant</b>: this package is the <em>only</em> place in the codebase that imports
 * {@code jdk.incubator.vector.*}. The classes here are loaded reflectively by
 * {@code RealVectorPrimitives.selectBackend()} so the rest of the module can be compiled and run
 * without the incubator module being available on the JVM. If the module is missing at runtime,
 * loading {@code SimdBackend} throws {@code NoClassDefFoundError} and the dispatcher falls back to
 * the scalar implementation in the parent package.
 * <p>
 * Build configuration must pass {@code --add-modules jdk.incubator.vector} to the
 * {@code compileJava} task that compiles this package. To exercise the SIMD path at runtime, the
 * same flag must be passed to the JVM.
 */
package com.apple.foundationdb.linear.simd;
