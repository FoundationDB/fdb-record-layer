/*
 * CascadesQueryTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
 * Class covering yamsql tests that exercise the Cascades query planner and its execution on the SQL/Relational
 * path, as distinct from the general integration coverage in {@link YamlIntegrationTests}.
 */
@YamlTest
public class CascadesQueryTests {

    @TestTemplate
    public void inQueryOrOverlap(YamlTest.Runner runner) throws Exception {
        runner.runYamsql("cascades-query-tests/in-query-or-overlap.yamsql");
    }
}
