/*
 * CascadesQueryTests.java
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

import com.apple.foundationdb.relational.yamltests.YamlTest;
import org.junit.jupiter.api.TestTemplate;

/**
 * Query-planning coverage ported from the Cascades tests in {@code fdb-record-layer-core}'s
 * {@code com.apple.foundationdb.record.provider.foundationdb.query} package.
 *
 * <p>
 * Those tests drive the Cascades planner through the {@code RecordQuery} builder API; the SQL layer
 * drives the same planner from SQL text, so most of them can be expressed declaratively here
 * instead. Each yamsql file corresponds to one source test class, and every query carries a comment
 * naming the source test method it came from. Where a source class needs more than one index
 * configuration the file is split, with the variant named in a suffix — the planner has to be forced
 * into a plan by the schema it is given, and {@code USE INDEX} cannot stand in for a missing index
 * because a hinted query drops the primary-scan candidate entirely.
 * </p>
 *
 * <p>
 * See {@code cascades-query-tests/README.md} for the source-class mapping, the authoring
 * conventions, and the list of tests that cannot be ported.
 * </p>
 */
@YamlTest
class CascadesQueryTests {
    private static final String PREFIX = "cascades-query-tests";

    @TestTemplate
    void modificationQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/modification-query.yamsql");
    }
}
