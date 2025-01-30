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

import com.apple.foundationdb.relational.yamltests.YamlTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;

/**
 * Class covering the standard integration tests specified by yamsql files.
 */
@YamlTest
public class YamlIntegrationTests {
    @TestTemplate
    public void showcasingTests(YamlTest.Runner runner) throws Exception {
        runner.run("showcasing-tests.yamsql");
    }

    @TestTemplate
    public void groupByTests(YamlTest.Runner runner) throws Exception {
        runner.run("groupby-tests.yamsql");
    }

    @TestTemplate
    public void standardTests(YamlTest.Runner runner) throws Exception {
        runner.run("standard-tests.yamsql");
    }

    @TestTemplate
    public void standardTestsWithProto(YamlTest.Runner runner) throws Exception {
        runner.run("standard-tests-proto.yamsql");
    }

    @TestTemplate
    public void fieldIndexTestsProto(YamlTest.Runner runner) throws Exception {
        runner.run("field-index-tests-proto.yamsql");
    }

    @TestTemplate
    public void standardTestsWithMetaData(YamlTest.Runner runner) throws Exception {
        runner.run("standard-tests-metadata.yamsql");
    }

    @TestTemplate
    public void nullOperator(YamlTest.Runner runner) throws Exception {
        runner.run("null-operator-tests.yamsql");
    }

    @TestTemplate
    @Disabled // TODO ([Wave 1] Relational returns deprecated fields for SELECT *)
    public void deprecatedFieldsTestsWithProto(YamlTest.Runner runner) throws Exception {
        runner.run("deprecated-fields-tests-proto.yamsql");
    }

    @TestTemplate
    public void versionsTests(YamlTest.Runner runner) throws Exception {
        runner.run("versions-tests.yamsql");
    }

    @TestTemplate
    public void scenarioTests(YamlTest.Runner runner) throws Exception {
        runner.run("scenario-tests.yamsql");
    }

    @TestTemplate
    public void joinTests(YamlTest.Runner runner) throws Exception {
        runner.run("join-tests.yamsql");
    }

    @TestTemplate
    public void subqueryTests(YamlTest.Runner runner) throws Exception {
        runner.run("subquery-tests.yamsql");
    }

    @TestTemplate
    public void selectAStar(YamlTest.Runner runner) throws Exception {
        runner.run("select-a-star.yamsql");
    }

    @TestTemplate
    public void insertsUpdatesDeletes(YamlTest.Runner runner) throws Exception {
        runner.run("inserts-updates-deletes.yamsql");
    }

    @TestTemplate
    @Disabled("TODO (Cannot insert into table after dropping and recreating schema template when using EmbeddedJDBCDriver)")
    public void createDropCreateTemplate(YamlTest.Runner runner) throws Exception {
        runner.run("create-drop-create-template.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTests(YamlTest.Runner runner) throws Exception {
        runner.run("aggregate-index-tests.yamsql");
    }

    @TestTemplate
    public void aggregateEmptyTable(YamlTest.Runner runner) throws Exception {
        runner.run("aggregate-empty-table.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTestsCount(YamlTest.Runner runner) throws Exception {
        runner.run("aggregate-index-tests-count.yamsql");
    }

    @TestTemplate
    public void aggregateIndexTestsCountEmpty(YamlTest.Runner runner) throws Exception {
        runner.run("aggregate-index-tests-count-empty.yamsql");
    }

    @TestTemplate
    public void maxRows(YamlTest.Runner runner) throws Exception {
        runner.run("maxRows.yamsql");
    }

    @TestTemplate
    public void nested(YamlTest.Runner runner) throws Exception {
        runner.run("nested-tests.yamsql");
    }

    @TestTemplate
    public void orderBy(YamlTest.Runner runner) throws Exception {
        runner.run("orderby.yamsql");
    }

    @TestTemplate
    public void primaryKey(YamlTest.Runner runner) throws Exception {
        runner.run("primary-key-tests.yamsql");
    }

    @TestTemplate
    public void sparseIndex(YamlTest.Runner runner) throws Exception {
        runner.run("sparse-index-tests.yamsql");
    }

    @TestTemplate
    public void disabledIndexWithProto(YamlTest.Runner runner) throws Exception {
        runner.run("disabled-index-tests-proto.yamsql");
    }

    @TestTemplate
    public void inPredicate(YamlTest.Runner runner) throws Exception {
        runner.run("in-predicate.yamsql");
    }

    @TestTemplate
    void booleanTypes(YamlTest.Runner runner) throws Exception {
        runner.run("boolean.yamsql");
    }

    @TestTemplate
    void bytes(YamlTest.Runner runner) throws Exception {
        runner.run("bytes.yamsql");
    }

    @TestTemplate
    void catalog(YamlTest.Runner runner) throws Exception {
        runner.run("catalog.yamsql");
    }

    @TestTemplate
    public void caseWhen(YamlTest.Runner runner) throws Exception {
        runner.run("case-when.yamsql");
    }

    @TestTemplate
    public void updateDeleteReturning(YamlTest.Runner runner) throws Exception {
        runner.run("update-delete-returning.yamsql");
    }

    @TestTemplate
    void like(YamlTest.Runner runner) throws Exception {
        runner.run("like.yamsql");
    }

    @TestTemplate
    void functions(YamlTest.Runner runner) throws Exception {
        runner.run("functions.yamsql");
    }

    @TestTemplate
    void createDrop(YamlTest.Runner runner) throws Exception {
        runner.run("create-drop.yamsql");
    }

    @TestTemplate
    void arrays(YamlTest.Runner runner) throws Exception {
        runner.run("arrays.yamsql");
    }

    @TestTemplate
    public void insertEnum(YamlTest.Runner runner) throws Exception {
        runner.run("insert-enum.yamsql");
    }

    @TestTemplate
    public void prepared(YamlTest.Runner runner) throws Exception {
        runner.run("prepared.yamsql");
    }

    @TestTemplate
    public void indexedFunctions(YamlTest.Runner runner) throws Exception {
        runner.run("indexed-functions.yamsql");
    }

    @TestTemplate
    public void union(YamlTest.Runner runner) throws Exception {
        runner.run("union.yamsql");
    }

    @TestTemplate
    public void unionEmptyTables(YamlTest.Runner runner) throws Exception {
        runner.run("union-empty-tables.yamsql");
    }

    @TestTemplate
    public void cte(YamlTest.Runner runner) throws Exception {
        runner.run("cte.yamsql");
    }

    @TestTemplate
    public void bitmap(YamlTest.Runner runner) throws Exception {
        runner.run("bitmap-aggregate-index.yamsql");
    }

    @TestTemplate
    public void recursiveCte(YamlTest.Runner runner) throws Exception {
        runner.run("recursive-cte.yamsql");
    }

    @TestTemplate
    public void enumTest(YamlTest.Runner runner) throws Exception {
        runner.run("enum.yamsql");
    }
}
