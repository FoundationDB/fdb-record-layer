/*
 * YamlIntegrationDisabledPlannerRewritesTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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
 * Class covering the standard integration tests specified by yamsql files. This class temporarily provides coverage
 * for the test suite of queries we normally run as part of CI, with the caveat that all rules in
 * {@link com.apple.foundationdb.record.query.plan.cascades.RewritingRuleSet} that are deemed optional are disabled.
 * <br>
 * Note: Use {@link MaintainYamlTestConfig} using {@link YamlTestConfigFilters#CORRECT_EXPLAIN_AND_METRICS} or similar
 *       to correct explain strings and/or planner metrics. That annotation works both on class and on method level.
 */
@YamlTest
public class YamlIntegrationDisabledPlannerRewritesTests {
    private static final String PREFIX = "disabled-planner-rewrites";

    @TestTemplate
    public void showcasingTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/showcasing-tests.yamsql");
    }

    @TestTemplate
    public void groupByTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/groupby-tests.yamsql");
    }

    @TestTemplate
    public void standardTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/standard-tests.yamsql");
    }

    @TestTemplate
    public void standardTestsWithProto(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/standard-tests-proto.yamsql");
    }

    @TestTemplate
    public void fieldIndexTestsProto(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/field-index-tests-proto.yamsql");
    }

    @TestTemplate
    public void standardTestsWithMetaData(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/standard-tests-metadata.yamsql");
    }

    @TestTemplate
    public void nullOperator(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/null-operator-tests.yamsql");
    }

    @TestTemplate
    public void versionsTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/versions-tests.yamsql");
    }

    @TestTemplate
    public void scenarioTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/scenario-tests.yamsql");
    }

    @TestTemplate
    public void joinTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/join-tests.yamsql");
    }

    @TestTemplate
    public void subqueryTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/subquery-tests.yamsql");
    }

    @TestTemplate
    public void selectAStar(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/select-a-star.yamsql");
    }

    @TestTemplate
    public void insertsUpdatesDeletes(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/inserts-updates-deletes.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTests(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/aggregate-index-tests.yamsql");
    }

    @TestTemplate
    public void aggregateEmptyTable(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/aggregate-empty-table.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTestsCount(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/aggregate-index-tests-count.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTestsCountEmpty(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/aggregate-index-tests-count-empty.yamsql");
    }

    @TestTemplate
    public void maxRows(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/maxRows.yamsql");
    }

    @TestTemplate
    public void nested(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-tests.yamsql");
    }

    @TestTemplate
    public void nestedWithNulls(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-with-nulls.yamsql");
    }

    @TestTemplate
    public void nestedWithNullsProto(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-with-nulls-proto.yamsql");
    }

    @TestTemplate
    public void orderBy(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/orderby.yamsql");
    }

    @TestTemplate
    public void primaryKey(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/primary-key-tests.yamsql");
    }

    @TestTemplate
    public void sparseIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sparse-index-tests.yamsql");
    }

    @TestTemplate
    public void disabledIndexWithProto(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/disabled-index-tests-proto.yamsql");
    }

    @TestTemplate
    public void inPredicate(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-predicate.yamsql");
    }

    @TestTemplate
    void booleanTypes(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/boolean.yamsql");
    }

    @TestTemplate
    void bytes(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/bytes.yamsql");
    }

    @TestTemplate
    void catalog(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/catalog.yamsql");
    }

    @TestTemplate
    public void caseWhen(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/case-when.yamsql");
    }

    @TestTemplate
    public void updateDeleteReturning(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/update-delete-returning.yamsql");
    }

    @TestTemplate
    void like(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/like.yamsql");
    }

    @TestTemplate
    void functions(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/functions.yamsql");
    }

    @TestTemplate
    void createDrop(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/create-drop.yamsql");
    }

    @TestTemplate
    void arrays(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/arrays.yamsql");
    }

    @TestTemplate
    public void insertEnum(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/insert-enum.yamsql");
    }

    @TestTemplate
    public void prepared(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/prepared.yamsql");
    }

    @TestTemplate
    public void indexedFunctions(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/indexed-functions.yamsql");
    }

    @TestTemplate
    public void union(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/union.yamsql");
    }

    @TestTemplate
    public void unionEmptyTables(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/union-empty-tables.yamsql");
    }

    @TestTemplate
    public void cte(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/cte.yamsql");
    }

    @TestTemplate
    public void bitmap(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/bitmap-aggregate-index.yamsql");
    }

    @TestTemplate
    public void recursiveCte(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/recursive-cte.yamsql");
    }

    @TestTemplate
    public void enumTest(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/enum.yamsql");
    }

    @TestTemplate
    public void uuidTest(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/uuid.yamsql");
    }

    @TestTemplate
    public void tableFunctionsTest(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/table-functions.yamsql");
    }

    @TestTemplate
    public void betweenTest(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/between.yamsql");
    }

    @TestTemplate
    public void sqlFunctionsTest(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sql-functions.yamsql");
    }
}
