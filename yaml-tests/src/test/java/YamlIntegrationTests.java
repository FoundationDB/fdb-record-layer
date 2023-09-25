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

public class YamlIntegrationTests extends YamlTestBase {
    @Test
    public void showcasingTests() throws Exception {
        doRun("showcasing-tests.yaml");
    }

    @Test
    public void groupbyTests() throws Exception {
        doRun("groupby-tests.yaml");
    }

    @Test
    public void standardTests() throws Exception {
        doRun("standard-tests.yaml");
    }

    @Test
    public void standardTestsWithProto() throws Exception {
        doRun("standard-tests-proto.yaml");
    }
    
    @Test
    public void standardTestsWithMetaData() throws Exception {
        doRun("standard-tests-metadata.yaml");
    }

    @Test
    public void nullOperator() throws Exception {
        doRun("null-operator-tests.yaml");
    }

    @Disabled // TODO ([Wave 1] Relational returns deprecated fields for SELECT *)
    public void deprecatedFieldsTestsWithProto() throws Exception {
        doRun("deprecated-fields-tests-proto.yaml");
    }

    @Test
    public void versionsTests() throws Exception {
        doRun("versions-tests.yaml");
    }

    @Test
    public void scenarioTests() throws Exception {
        doRun("scenario-tests.yaml");
    }

    @Test
    public void joinTests() throws Exception {
        doRun("join-tests.yaml");
    }

    @Test
    public void subqueryTests() throws Exception {
        doRun("subquery-tests.yaml");
    }

    @Test
    public void selectAStar() throws Exception {
        doRun("select-a-star.yaml");
    }

    @Test
    public void insertsUpdatesDeletes() throws Exception {
        doRun("inserts-updates-deletes.yaml");
    }

    @Test
    @Disabled("TODO (Cannot insert into table after dropping and recreating schema template when using EmbeddedJDBCDriver)")
    public void createDropCreateTemplate() throws Exception {
        doRun("create-drop-create-template.yaml");
    }

    @Test
    public void aggregateIndexTests() throws Exception {
        doRun("aggregate-index-tests.yaml");
    }

    @Test
    public void aggregateEmptyTable() throws Exception {
        doRun("aggregate-empty-table.yaml");
    }

    @Test
    public void aggregateIndexTestsCount() throws Exception {
        doRun("aggregate-index-tests-count.yaml");
    }

    @Test
    public void aggregateIndexTestsCountEmpty() throws Exception {
        doRun("aggregate-index-tests-count-empty.yaml");
    }

    @Test
    public void limit() throws Exception {
        doRun("limit.yaml");
    }

    @Test
    public void nested() throws Exception {
        doRun("nested-tests.yaml");
    }

    @Test
    public void orderBy() throws Exception {
        doRun("orderby.yaml");
    }

    @Test
    public void primaryKey() throws Exception {
        doRun("primary-key-tests.yaml");
    }

    @Test
    public void sparseIndex() throws Exception {
        doRun("sparse-index-tests.yaml");
    }

    @Test
    public void disabledIndexWithProto() throws Exception {
        doRun("disabled-index-tests-proto.yaml");
    }

    @Test
    public void inPredicate() throws Exception {
        doRun("in-predicate.yaml");
    }

    @Test
    void booleanTypes() throws Exception {
        doRun("boolean.yaml");
    }

    @Test
    void catalog() throws Exception {
        doRun("catalog.yaml");
    }

    @Test
    public void caseWhen() throws Exception {
        doRun("case-when.yaml");
    }

    @Test
    public void updateDeleteReturning() throws Exception {
        doRun("update-delete-returning.yaml");
    }

    @Test
    void like() throws Exception {
        doRun("like.yaml");
    }
    
    @Test
    void functions() throws Exception {
        doRun("functions.yaml");
    }

    @Test
    void createDrop() throws Exception {
        doRun("create-drop.yaml");
    }

    @Test
    void arrays() throws Exception {
        doRun("arrays.yaml");
    }
}
