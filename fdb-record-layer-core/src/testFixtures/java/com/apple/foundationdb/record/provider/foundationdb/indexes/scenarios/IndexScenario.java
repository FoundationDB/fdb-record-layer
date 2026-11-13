/*
 * IndexScenario.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;

import java.util.function.Supplier;

/**
 * A sequence of operations that an index maintainer should handle correctly, independent of the
 * index type, for example building an index while records are being written, or deleting a group of
 * records. Every scenario is run against every {@link IndexDefinition}.
 *
 * <p>Scenarios are discovered with {@link java.util.ServiceLoader}, so an implementation needs a public
 * no-argument constructor, and must be registered as a service provider for this interface, typically
 * with {@code @AutoService(IndexScenario.class)}. A scenario should be stateless, as one instance is
 * shared by every test that runs it.
 *
 * <p>A scenario drives the store through an {@link IndexScenarioModel}, and should only assert what holds
 * for any index type. When it relies on something only some maintainers support, it should check the
 * relevant {@link IndexDefinition} capability (such as {@link IndexDefinition#supportsGrouping()})
 * and skip with {@link org.junit.jupiter.api.Assumptions#assumeTrue(boolean, String)} if it is
 * missing.
 */
public interface IndexScenario {
    /**
     * Run this scenario against an index.
     *
     * @param definition the index type under test
     * @param openContext opens a new transaction against the database the test uses
     * @param storeBuilder a builder for the record store to run against. It should identify the store
     * (for example with a key space path), but not set the metadata, which the scenario supplies.
     * @throws Exception if the scenario fails with an exception, rather than an assertion
     */
    void runTest(IndexDefinition definition,
                 Supplier<FDBRecordContext> openContext,
                 FDBRecordStore.Builder storeBuilder) throws Exception;
}
