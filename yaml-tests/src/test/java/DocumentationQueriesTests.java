/*
 * DocumentationQueriesTests.java
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

import com.apple.foundationdb.relational.yamltests.YamlTest;
import org.junit.jupiter.api.TestTemplate;

/**
 * Test suite that aims to run all sample SQL queries defined in the documentation. This ensures
 * that as SQL syntax and capabilities evolve, the documentation remains consistent and up to date.
 */
@YamlTest
class DocumentationQueriesTests {
    private static final String PREFIX = "documentation-queries";

    @TestTemplate
    void withDocumentationQueriesTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/with-documentation-queries.yamsql");
    }

    @TestTemplate
    void castDocumentationQueriesTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/cast-documentation-queries.yamsql");
    }

    @TestTemplate
    void vectorDocumentationQueriesTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/vector-documentation-queries.yamsql");
    }
}
