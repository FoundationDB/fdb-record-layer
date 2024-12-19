/*
 * JDBCYamlIntegrationTests.java
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
 * An extension of {@link YamlIntegrationTests} that disables tests that do not work when running
 * through JDBC.
 */
public abstract class JDBCYamlIntegrationTests extends YamlIntegrationTests {
    @Override
    @Test
    @Disabled("The field-index-tests-proto.yaml has 'load template' which is not supported")
    public void fieldIndexTestsProto() throws Exception {
        doRun("field-index-tests-proto.yamsql");
    }

    @Override
    @Test
    @Disabled("The standard-tests-proto.yaml has 'load template' which is not supported")
    public void standardTestsWithProto() throws Exception {
        super.standardTestsWithProto();
    }

    @Override
    @Test
    @Disabled("The standard-tests-metadata.yaml has 'load template' which is not supported")
    public void standardTestsWithMetaData() throws Exception {
        super.standardTestsWithMetaData();
    }

    @Override
    @Test
    @Disabled("The disabled-index-tests-proto.yaml has 'load schema template' which is not supported")
    public void disabledIndexWithProto() throws Exception {
        super.disabledIndexWithProto();
    }

    @Override
    @Test
    @Disabled("The deprecated-fields-tests-proto.yaml has 'load schema template' which is not supported")
    public void deprecatedFieldsTestsWithProto() throws Exception {
        super.deprecatedFieldsTestsWithProto();
    }

    @Override
    @Test
    @Disabled("TODO: Flakey")
    public void orderBy() throws Exception {
        super.orderBy();
    }

    @Override
    @Test
    @Disabled("TODO: Flakey")
    public void scenarioTests() throws Exception {
        super.scenarioTests();
    }

    @Override
    @Test
    @Disabled("Requires continuation support")
    public void versionsTests() throws Exception {
        super.versionsTests();
    }

    @Override
    @Test
    @Disabled("TODO: Need to work on supporting labels")
    public void maxRows() throws Exception {
        super.maxRows();
    }

    @Override
    @Test
    @Disabled("TODO")
    public void selectAStar() throws Exception {
        super.selectAStar();
    }

    @Override
    @Test
    @Disabled("TODO: Flakey")
    public void aggregateIndexTestsCount() throws Exception {
        super.aggregateIndexTestsCount();
    }

    @Override
    @Test
    @Disabled("TODO: Flakey")
    public void joinTests() throws Exception {
        super.joinTests();
    }

    @Override
    @Test
    @Disabled("TODO: Flakey")
    public void nested() throws Exception {
        super.nested();
    }

    @Override
    @Test
    @Disabled("TODO: Flakey")
    public void showcasingTests() throws Exception {
        super.showcasingTests();
    }

    @Override
    @Test
    @Disabled("TODO: Not supported")
    public void insertEnum() throws Exception {
        super.insertEnum();
    }

    @Override
    @Test
    @Disabled("TODO: Not supported")
    public void prepared() throws Exception {
        super.prepared();
    }

    @Override
    @Test
    @Disabled("TODO: Continuations not supported in JDBC")
    public void joinFilteredTest() throws Exception {
        doRun("join-filtered.yamsql");
    }
}
