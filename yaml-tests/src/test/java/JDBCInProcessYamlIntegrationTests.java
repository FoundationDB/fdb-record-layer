/*
 * JDBCInProcessYamlIntegrationTests.java
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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.jdbc.JDBCURI;
import com.apple.foundationdb.relational.server.InProcessRelationalServer;
import com.apple.foundationdb.relational.yamltests.YamlRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import javax.annotation.Nullable;
import java.net.URI;
import java.sql.DriverManager;

/**
 * Like {@link EmbeddedYamlIntegrationTests} only it runs the YAML via the fdb-relational-jdbc client
 * talking to an in-process Relational Server.
 */
public class JDBCInProcessYamlIntegrationTests extends JDBCYamlIntegrationTests {
    private static final Logger LOG = LogManager.getLogger(JDBCInProcessYamlIntegrationTests.class);

    @Nullable
    private static InProcessRelationalServer server;

    @BeforeAll
    public static void beforeAll() {
        try {
            server = new InProcessRelationalServer().start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void afterAll() {
        if (server != null) {
            try {
                server.close();
                server = null;
            } catch (Exception e) {
                throw new RelationalException(e.getMessage(), ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
            }
        }
    }

    @Override
    YamlRunner.YamlConnectionFactory createConnectionFactory() {
        return connectPath -> {
            // Add name of the in-process running server to the connectPath.
            URI connectPathPlusServerName = JDBCURI.addQueryParameter(connectPath, JDBCURI.INPROCESS_URI_QUERY_SERVERNAME_KEY, server.getServerName());
            String uriStr = connectPathPlusServerName.toString().replaceFirst("embed:", "relational://");
            LOG.info("Rewrote {} as {}", connectPath, uriStr);
            return DriverManager.getConnection(uriStr).unwrap(RelationalConnection.class);
        };
    }

}
