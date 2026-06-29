/*
 * YamlIntegrationTests.java
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

import com.apple.foundationdb.relational.yamltests.MaintainYamlTestConfig;
import com.apple.foundationdb.relational.yamltests.YamlTest;
import com.apple.foundationdb.relational.yamltests.YamlTestConfigFilters;
import org.junit.jupiter.api.TestTemplate;

/**
 * Class covering the standard integration tests specified by yamsql files.
 * <br>
 * Note: Use {@link MaintainYamlTestConfig} using {@link YamlTestConfigFilters#CORRECT_EXPECTATIONS} or similar
 * to correct explain strings and/or planner metrics. That annotation works both on class and on method level.
 * Note: Use {@link com.apple.foundationdb.relational.yamltests.DebugPlanner} on a specific test in this class to bring
 * up the {@link com.apple.foundationdb.record.query.plan.cascades.debug.PlannerRepl} debugger implementation.
 */
@YamlTest
public class YamlIntegrationTests {

    @TestTemplate
    public void showcasingTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql("showcasing-tests.yamsql");
    }

}
