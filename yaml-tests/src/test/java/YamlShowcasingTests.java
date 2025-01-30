/*
 * YamlShowcasingTests.java
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
 * Temporary test fixture to validate {@link YamlTest} before replacing {@link YamlIntegrationTests}.
 */
@YamlTest
public class YamlShowcasingTests {
    @TestTemplate
    public void showcasingTests(YamlTest.Runner runner) throws Exception {
        runner.run("showcasing-tests.yamsql");
    }

    @TestTemplate
    public void enumTest(YamlTest.Runner runner) throws Exception {
        runner.run("enum.yamsql");
    }
}
