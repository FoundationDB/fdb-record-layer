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
 * Index Scenarios provides a functionality for easily testing a variety of index related
 * scenarios for a given {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer}.
 * The key entry point is the argument provider: {@link com.apple.foundationdb.record.provider.foundationdb.indexes.IndexScenarios}
 * which should be added as a source for an {@link org.junit.jupiter.params.ParameterizedTest}.
 * <p>
 * An index type is described once by an
 * {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition}, and
 * every registered {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenario}
 * is then run against it:
 * <pre>
 * &#64;ParameterizedTest
 * &#64;IndexScenarios
 * void indexScenariosTest(IndexScenario scenario) throws Exception {
 *     scenario.runTest(
 *             ValueIndexDefinition::new,
 *             () -&gt; dbExtension.getDatabase().openContext(),
 *             FDBRecordStore.newBuilder().setKeySpacePath(path));
 * }
 * </pre>
 * See the {@code README.md} in this package for the design.
 */
package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;
