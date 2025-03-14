/*
 * YamlIntegrationTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.YamlTest;
import com.apple.foundationdb.relational.yamltests.YamlTestConfigFilters;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;

/**
 * Class covering the standard integration tests specified by yamsql files.
 * <br>
 * Note: Use {@link MaintainYamlTestConfig} using {@link YamlTestConfigFilters#CORRECT_EXPLAIN_AND_METRICS} or similar
 *       to correct explain strings and/or planner metrics. That annotation works both on class and on method level.
 */
@YamlTest
public class YamlIntegrationTests {
    @TestTemplate
    public void showcasingTests(YamlRunner runner) throws Exception {
        runner.runYamsql("showcasing-tests.yamsql");
    }

    @TestTemplate
    public void groupByTests(YamlRunner runner) throws Exception {
        runner.runYamsql("groupby-tests.yamsql");
    }

    @TestTemplate
    public void standardTests(YamlRunner runner) throws Exception {
        runner.runYamsql("standard-tests.yamsql");
    }

    @TestTemplate
    public void standardTestsWithProto(YamlRunner runner) throws Exception {
        runner.runYamsql("standard-tests-proto.yamsql");
    }

    @TestTemplate
    public void fieldIndexTestsProto(YamlRunner runner) throws Exception {
        runner.runYamsql("field-index-tests-proto.yamsql");
    }

    @TestTemplate
    public void standardTestsWithMetaData(YamlRunner runner) throws Exception {
        runner.runYamsql("standard-tests-metadata.yamsql");
    }

    @TestTemplate
    public void nullOperator(YamlRunner runner) throws Exception {
        runner.runYamsql("null-operator-tests.yamsql");
    }

    @TestTemplate
    @Disabled // TODO ([Wave 1] Relational returns deprecated fields for SELECT *)
    public void deprecatedFieldsTestsWithProto(YamlRunner runner) throws Exception {
        runner.runYamsql("deprecated-fields-tests-proto.yamsql");
    }

    @TestTemplate
    public void versionsTests(YamlRunner runner) throws Exception {
        runner.runYamsql("versions-tests.yamsql");
    }

    @TestTemplate
    public void scenarioTests(YamlRunner runner) throws Exception {
        runner.runYamsql("scenario-tests.yamsql");
    }

    @TestTemplate
    public void joinTests(YamlRunner runner) throws Exception {
        runner.runYamsql("join-tests.yamsql");
    }

    @TestTemplate
    public void subqueryTests(YamlRunner runner) throws Exception {
        runner.runYamsql("subquery-tests.yamsql");
    }

    @TestTemplate
    public void selectAStar(YamlRunner runner) throws Exception {
        runner.runYamsql("select-a-star.yamsql");
    }

    @TestTemplate
    public void insertsUpdatesDeletes(YamlRunner runner) throws Exception {
        runner.runYamsql("inserts-updates-deletes.yamsql");
    }

    @TestTemplate
    @Disabled("TODO (Cannot insert into table after dropping and recreating schema template when using EmbeddedJDBCDriver)")
    public void createDropCreateTemplate(YamlRunner runner) throws Exception {
        runner.runYamsql("create-drop-create-template.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTests(YamlRunner runner) throws Exception {
        runner.runYamsql("aggregate-index-tests.yamsql");
    }

    @TestTemplate
    public void aggregateEmptyTable(YamlRunner runner) throws Exception {
        runner.runYamsql("aggregate-empty-table.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTestsCount(YamlRunner runner) throws Exception {
        runner.runYamsql("aggregate-index-tests-count.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTestsCountEmpty(YamlRunner runner) throws Exception {
        runner.runYamsql("aggregate-index-tests-count-empty.yamsql");
    }

    @TestTemplate
    public void maxRows(YamlRunner runner) throws Exception {
        runner.runYamsql("maxRows.yamsql");
    }

    @TestTemplate
    public void nested(YamlRunner runner) throws Exception {
        runner.runYamsql("nested-tests.yamsql");
    }

    @TestTemplate
    public void orderBy(YamlRunner runner) throws Exception {
        runner.runYamsql("orderby.yamsql");
    }

    @TestTemplate
    public void primaryKey(YamlRunner runner) throws Exception {
        runner.runYamsql("primary-key-tests.yamsql");
    }

    @TestTemplate
    public void sparseIndex(YamlRunner runner) throws Exception {
        runner.runYamsql("sparse-index-tests.yamsql");
    }

    @TestTemplate
    public void disabledIndexWithProto(YamlRunner runner) throws Exception {
        runner.runYamsql("disabled-index-tests-proto.yamsql");
    }

    @TestTemplate
    public void inPredicate(YamlRunner runner) throws Exception {
        runner.runYamsql("in-predicate.yamsql");
    }

    @TestTemplate
    void booleanTypes(YamlRunner runner) throws Exception {
        runner.runYamsql("boolean.yamsql");
    }

    @TestTemplate
    void bytes(YamlRunner runner) throws Exception {
        runner.runYamsql("bytes.yamsql");
    }

    @TestTemplate
    void catalog(YamlRunner runner) throws Exception {
        runner.runYamsql("catalog.yamsql");
    }

    @TestTemplate
    public void caseWhen(YamlRunner runner) throws Exception {
        runner.runYamsql("case-when.yamsql");
    }

    @TestTemplate
    public void updateDeleteReturning(YamlRunner runner) throws Exception {
        runner.runYamsql("update-delete-returning.yamsql");
    }

    @TestTemplate
    void like(YamlRunner runner) throws Exception {
        runner.runYamsql("like.yamsql");
    }

    @TestTemplate
    void functions(YamlRunner runner) throws Exception {
        runner.runYamsql("functions.yamsql");
    }

    @TestTemplate
    void createDrop(YamlRunner runner) throws Exception {
        runner.runYamsql("create-drop.yamsql");
    }

    @TestTemplate
    void arrays(YamlRunner runner) throws Exception {
        runner.runYamsql("arrays.yamsql");
    }

    @TestTemplate
    public void insertEnum(YamlRunner runner) throws Exception {
        runner.runYamsql("insert-enum.yamsql");
    }

    @TestTemplate
    public void prepared(YamlRunner runner) throws Exception {
        runner.runYamsql("prepared.yamsql");
    }

    @TestTemplate
    public void indexedFunctions(YamlRunner runner) throws Exception {
        runner.runYamsql("indexed-functions.yamsql");
    }

    @TestTemplate
    public void union(YamlRunner runner) throws Exception {
        runner.runYamsql("union.yamsql");
    }

    @TestTemplate
    public void unionEmptyTables(YamlRunner runner) throws Exception {
        runner.runYamsql("union-empty-tables.yamsql");
    }

    @TestTemplate
    public void cte(YamlRunner runner) throws Exception {
        runner.runYamsql("cte.yamsql");
    }

    @TestTemplate
    public void bitmap(YamlRunner runner) throws Exception {
        runner.runYamsql("bitmap-aggregate-index.yamsql");
    }

    @TestTemplate
    public void recursiveCte(YamlRunner runner) throws Exception {
        runner.runYamsql("recursive-cte.yamsql");
    }

    @TestTemplate
    public void enumTest(YamlRunner runner) throws Exception {
        runner.runYamsql("enum.yamsql");
    }
}
