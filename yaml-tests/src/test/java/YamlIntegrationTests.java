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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Base class covering all the integration tests specified by yamsql files.
 */
public abstract class YamlIntegrationTests extends YamlTestBase {
    @Test
    public void showcasingTests() throws Exception {
        doRun("showcasing-tests.yamsql");
    }

    @Test
    public void groupByTests() throws Exception {
        doRun("groupby-tests.yamsql");
    }

    @Test
    public void standardTests() throws Exception {
        doRun("standard-tests.yamsql");
    }

    @Test
    public void standardTestsWithProto() throws Exception {
        doRun("standard-tests-proto.yamsql");
    }

    @Test
    public void fieldIndexTestsProto() throws Exception {
        doRun("field-index-tests-proto.yamsql");
    }

    @Test
    public void standardTestsWithMetaData() throws Exception {
        doRun("standard-tests-metadata.yamsql");
    }

    @Test
    public void nullOperator() throws Exception {
        doRun("null-operator-tests.yamsql");
    }

    @Test
    @Disabled // TODO ([Wave 1] Relational returns deprecated fields for SELECT *)
    public void deprecatedFieldsTestsWithProto() throws Exception {
        doRun("deprecated-fields-tests-proto.yamsql");
    }

    @Test
    public void versionsTests() throws Exception {
        doRun("versions-tests.yamsql");
    }

    @Test
    public void scenarioTests() throws Exception {
        doRun("scenario-tests.yamsql");
    }

    @Test
    public void joinTests() throws Exception {
        doRun("join-tests.yamsql");
    }

    @Test
    public void subqueryTests() throws Exception {
        doRun("subquery-tests.yamsql");
    }

    @Test
    public void selectAStar() throws Exception {
        doRun("select-a-star.yamsql");
    }

    @Test
    public void insertsUpdatesDeletes() throws Exception {
        doRun("inserts-updates-deletes.yamsql");
    }

    @Test
    @Disabled("TODO (Cannot insert into table after dropping and recreating schema template when using EmbeddedJDBCDriver)")
    public void createDropCreateTemplate() throws Exception {
        doRun("create-drop-create-template.yamsql");
    }

    @Test
    public void aggregateIndexTests() throws Exception {
        doRun("aggregate-index-tests.yamsql");
    }

    @Test
    public void aggregateEmptyTable() throws Exception {
        doRun("aggregate-empty-table.yamsql");
    }

    @Test
    public void aggregateIndexTestsCount() throws Exception {
        doRun("aggregate-index-tests-count.yamsql");
    }

    @Test
    public void aggregateIndexTestsCountEmpty() throws Exception {
        doRun("aggregate-index-tests-count-empty.yamsql");
    }

    @Test
    public void maxRows() throws Exception {
        doRun("maxRows.yamsql");
    }

    @Test
    public void nested() throws Exception {
        doRun("nested-tests.yamsql");
    }

    @Test
    public void orderBy() throws Exception {
        doRun("orderby.yamsql");
    }

    @Test
    public void primaryKey() throws Exception {
        doRun("primary-key-tests.yamsql");
    }

    @Test
    public void sparseIndex() throws Exception {
        doRun("sparse-index-tests.yamsql");
    }

    @Test
    public void disabledIndexWithProto() throws Exception {
        doRun("disabled-index-tests-proto.yamsql");
    }

    @Test
    public void inPredicate() throws Exception {
        doRun("in-predicate.yamsql");
    }

    @Test
    void booleanTypes() throws Exception {
        doRun("boolean.yamsql");
    }

    @Test
    void bytes() throws Exception {
        doRun("bytes.yamsql");
    }

    @Test
    void catalog() throws Exception {
        doRun("catalog.yamsql");
    }

    @Test
    public void caseWhen() throws Exception {
        doRun("case-when.yamsql");
    }

    @Test
    public void updateDeleteReturning() throws Exception {
        doRun("update-delete-returning.yamsql");
    }

    @Test
    void like() throws Exception {
        doRun("like.yamsql");
    }

    @Test
    void functions() throws Exception {
        doRun("functions.yamsql");
    }

    @Test
    void createDrop() throws Exception {
        doRun("create-drop.yamsql");
    }

    @Test
    void arrays() throws Exception {
        doRun("arrays.yamsql");
    }

    @Test
    public void insertEnum() throws Exception {
        doRun("insert-enum.yamsql");
    }

    @Test
    public void prepared() throws Exception {
        doRun("prepared.yamsql");
    }

    @Test
    public void indexedFunctions() throws Exception {
        doRun("indexed-functions.yamsql");
    }

    @Test
    public void union() throws Exception {
        doRun("union.yamsql");
    }

    @Test
    public void unionEmptyTables() throws Exception {
        doRun("union-empty-tables.yamsql");
    }

    @Test
    public void cte() throws Exception {
        doRun("cte.yamsql");
    }

    @Test
    public void bitmap() throws Exception {
        doRun("bitmap-aggregate-index.yamsql");
    }
}
