/*
 * ImportSchemaTemplateTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import org.junit.jupiter.api.TestTemplate;

@YamlTest
class ImportSchemaTemplateTests {

    /**
     * Test the logic that can load a meta-data object from JSON where the meta-data has a proto file descriptor with
     * dependencies. Validate that we can load the meta-data and then run basic queries against it.
     * See {@code MetaDataExportUtilityTests.createIncludedDependenciesMetaData} for how the meta-data used
     * in this test ({@code with_included_dependencies_metadata.json}) is generated.
     *
     * @param runner YAML runner
     * @throws Exception from the test execution
     */
    @TestTemplate
    void withIncludedDependencies(YamlTest.Runner runner) throws Exception {
        runner.runYamsql("import-schema-template/with-included-dependencies.yamsql");
    }

    /**
     * Test the logic that can load a meta-data object from JSON where the meta-data
     * has raw SQL user-defined functions included in it. Make sure that both user-defined macro
     * functions and user-defined compiled SQL functions can both be loaded and used in basic
     * queries against the meta-data.
     *
     * @param runner YAML runner
     * @throws Exception from the test execution
     */
    @TestTemplate
    void withUserDefinedFunctions(YamlTest.Runner runner) throws Exception {
        runner.runYamsql("import-schema-template/with-user-defined-functions.yamsql");
    }

    /**
     * Test the logic that can load a meta-data object from JSON where the meta-data
     * has auxiliary types (i.e. user-defined SQL types) included in it. Make sure that
     * the loaded auxiliary types can be used as parameters of user-defined functions.
     *
     * @param runner YAML runner
     * @throws Exception from the test execution
     */
    @TestTemplate
    void withAuxiliaryTypes(YamlTest.Runner runner) throws Exception {
        runner.runYamsql("import-schema-template/with-auxiliary-types.yamsql");
    }
}
