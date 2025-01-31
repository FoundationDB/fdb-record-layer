/*
 * JDBCYamlIntegrationTests.java
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * An extension of {@link YamlIntegrationTests} that disables tests that do not work when running
 * through JDBC.
 */
public abstract class JDBCYamlIntegrationTests extends YamlIntegrationTests {

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
    @Disabled("TODO: sql.Type OTHER is not supported in the ResultSet")
    public void enumTest() throws Exception {
        super.enumTest();
    }
}
