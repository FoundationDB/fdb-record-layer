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
 * Test fixtures for the {@code Guardiann} clustered vector structure: structural-invariant assertions
 * ({@code GuardiannStructureAsserts}) and vector-dataset ({@code .fvecs}/{@code .ivecs}) loaders and paths
 * ({@code VecsDatasetLoaders}, {@code SiftTestHelpers}), shared between the fdb-extensions guardiann tests and the
 * record-layer's vector-index tests.
 */
package com.apple.foundationdb.async.guardiann;
