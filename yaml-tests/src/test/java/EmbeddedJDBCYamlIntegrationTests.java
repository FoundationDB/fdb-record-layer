/*
 * EmbeddedJDBCYamlIntegrationTests.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.yamltests.YamlRunner;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ServiceLoader;

/**
 * Like {@link YamlIntegrationTests} only it runs the YAML via the embedded fdb-relational-jdbc driver.
 */
// TODO: Run all yaml files. See below for reasons on why we skip particular yamls, usually because
// waiting on functionality.
public class EmbeddedJDBCYamlIntegrationTests extends YamlIntegrationTests {

    static {
        // Load ServiceLoader Services.
        for (Driver ignore : ServiceLoader.load(Driver.class)) {
            // Intentionally empty
        }
    }

    @Override
    YamlRunner.YamlConnectionFactory createConnectionFactory() {
        return connectPath -> DriverManager.getDriver("jdbc:embed:///").connect(connectPath.toString(), null).unwrap(RelationalConnection.class);
    }

    @Override
    @Test
    @Disabled("TODO: Need to work on supporting labels")
    public void limit() throws Exception {
        super.limit();
    }
}
