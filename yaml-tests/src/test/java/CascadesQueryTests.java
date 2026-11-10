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
 * <p>Those tests drive the Cascades planner through the {@code RecordQuery} builder API; the SQL layer
 * drives the same planner from SQL text, so most of them can be expressed declaratively here
 * instead. Each yamsql file corresponds to one source test class, and every query carries a comment
 * naming the source test method it came from. Where a source class needs more than one index
 * configuration the file is split, with the variant named in a suffix — the planner has to be forced
 * into a plan by the schema it is given, and {@code USE INDEX} cannot stand in for a missing index
 * because a hinted query drops the primary-scan candidate entirely.
 *
 * <p>See {@code cascades-query-tests/README.md} for the source-class mapping, the authoring
 * conventions, and the list of tests that cannot be ported.
 */
@YamlTest
class CascadesQueryTests {
    private static final String PREFIX = "cascades-query-tests";

    @TestTemplate
    void inQueryCompoundIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-compound-index.yamsql");
    }

    @TestTemplate
    void inQueryHeader(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-header.yamsql");
    }

    @TestTemplate
    void inQueryMultiIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-multi-index.yamsql");
    }

    @TestTemplate
    void inQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query.yamsql");
    }

    @TestTemplate
    void inQueryOrCompound(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-or-compound.yamsql");
    }

    @TestTemplate
    void inQueryOr(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-or.yamsql");
    }

    @TestTemplate
    void inQueryUnionIntersectionNoNv3(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-union-intersection-no-nv3.yamsql");
    }

    @TestTemplate
    void inQueryUnionIntersection(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-union-intersection.yamsql");
    }

    @TestTemplate
    void inQueryUnionSargable(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-union-sargable.yamsql");
    }

    @TestTemplate
    void inQueryOrOverlap(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-or-overlap.yamsql");
    }

    @TestTemplate
    void permutedMinMaxNumValue2(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-num-value-2.yamsql");
    }

    @TestTemplate
    void permutedMinMaxOrderingKeys(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-ordering-keys.yamsql");
    }

    @TestTemplate
    void permutedMinMaxRepeater(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-repeater.yamsql");
    }

    @TestTemplate
    void permutedMinMax(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max.yamsql");
    }

    @TestTemplate
    void simpleQueryGraphNameTagIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph-name-tag-index.yamsql");
    }

    @TestTemplate
    void simpleQueryGraphTagIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph-tag-index.yamsql");
    }

    @TestTemplate
    void inQuerySortedExtraUnorderedColumns(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-sorted-extra-unordered-columns.yamsql");
    }
}
