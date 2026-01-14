/*
 * VectorTypeTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

public class VectorSqlTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public VectorSqlTest() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void selectFromHnsw() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE photos(zone string, recordId string, name string, embedding vector(3, half), primary key (zone, recordId)) " +
                "CREATE VIEW V1 AS SELECT embedding, zone, name from photos " +
                "CREATE VECTOR INDEX MV1 USING HNSW ON V1(embedding) PARTITION BY(zone, name)" +
                        " OPTIONS (METRIC = EUCLIDEAN_METRIC)";

        final var dataVector = new HalfRealVector(new double[] {1.2f, -0.3f, 3.14f});
        final var queryVector = new HalfRealVector(new double[] {1.2f, -0.4f, 3.14f});

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate).schemaTemplateOptions((new SchemaTemplateRule.SchemaTemplateOptions(true, true))).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("insert into photos values (?, ?, ?, ?)")) {
                statement.setString(1, "1");
                statement.setString(2, "100");
                statement.setString(3, "DarthVader");
                statement.setObject(4, dataVector);
                statement.executeUpdate();
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM photos WHERE zone = '1' and name = 'DarthVader' " +
                    "and row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC) < 10")) {
                statement.setObject(1, queryVector);
                final var resultSet = statement.executeQuery();
                resultSet.next();
                Assertions.assertThat(resultSet.getString(1)).isEqualTo("1");
                Assertions.assertThat(resultSet.getString(2)).isEqualTo("100");
                Assertions.assertThat(resultSet.getString(3)).isEqualTo("DarthVader");
                Assertions.assertThat(resultSet.getObject(4)).isEqualTo(dataVector);
            }
        }
    }

    @Test
    void selectFromHnswNegativeTest() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE photos(zone string, recordId string, name string, embedding vector(3, half), primary key (zone, recordId)) " +
                        "CREATE VIEW V1 AS SELECT embedding, zone, name from photos " +
                        "CREATE VECTOR INDEX MV1 USING HNSW ON V1(embedding) PARTITION BY(zone, name)" +
                        " OPTIONS (METRIC = COSINE_METRIC)";

        final var queryVector = new HalfRealVector(new double[] {1.2f, -0.4f, 3.14f});

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate).schemaTemplateOptions((new SchemaTemplateRule.SchemaTemplateOptions(true, true))).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM photos WHERE zone = '1' and name = 'DarthVader' " +
                    "and row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC) < 10")) {
                statement.setObject(1, queryVector);
                RelationalAssertions.assertThrowsSqlException(statement::executeQuery)
                        .containsInMessage("Cascades planner could not plan query")
                        .hasErrorCode(ErrorCode.UNSUPPORTED_QUERY);
            }
        }
    }
}
