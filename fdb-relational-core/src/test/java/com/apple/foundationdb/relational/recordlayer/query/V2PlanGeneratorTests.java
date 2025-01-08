/*
 * V2PlanGeneratorTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalStatement;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public class V2PlanGeneratorTests {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public V2PlanGeneratorTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void simpleQueryWorks() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select a from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(100, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleQuery2Works() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select a, b, c from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(100, resultSet.getInt(1));
                    Assertions.assertEquals(500, resultSet.getInt(2));
                    Assertions.assertEquals(101, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertEquals(501, resultSet.getInt(2));
                    Assertions.assertEquals(102, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleQuery3Works() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select t1.a, b as q, c from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(100, resultSet.getInt(1));
                    Assertions.assertEquals(500, resultSet.getInt(2));
                    Assertions.assertEquals(101, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertEquals(501, resultSet.getInt(2));
                    Assertions.assertEquals(102, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleSubquery4Works() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select x.a from (select a from t1) as x");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(100, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleSubquery5Works() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select a from (select a from t1) as x");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(100, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleSubquery6Works() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select m from (select y.c as m from (select b, c, a from t1 ) as y) as x");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(102, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleSubquery7Works() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 100, 500, 101), (43, 101, 501, 102)");
                relationalStatement.executeInternal("select c, a from (select a, b, c from t1) as x");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertEquals(100, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(102, resultSet.getInt(1));
                    Assertions.assertEquals(101, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleCorrelatedJoinWorks() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info array, b bigint array, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, [(30, (5000, 5500)), (34, (100, 200))], [500, 501], 101), (43, [(2, (2000, 2200)), (9, (2000,2400))], [700, 701], 102)");
                relationalStatement.executeInternal("select pk, y.ratings from  t1, t1.i as y");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(30, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(34, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(2, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(9, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest1() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select * from t2");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(400, resultSet.getInt(2));
                    Assertions.assertEquals(500, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(600, resultSet.getInt(2));
                    Assertions.assertEquals(400, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest2() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select t2.* from t2 options ( nocache )");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(400, resultSet.getInt(2));
                    Assertions.assertEquals(500, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(600, resultSet.getInt(2));
                    Assertions.assertEquals(400, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest3() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select * from (select t2.* from t2) as m");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(400, resultSet.getInt(2));
                    Assertions.assertEquals(500, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(600, resultSet.getInt(2));
                    Assertions.assertEquals(400, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest4() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select c, b from (select t1.* from t1) as m");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertEquals(500, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(102, resultSet.getInt(1));
                    Assertions.assertEquals(501, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest5() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select c, i.ratings, b from (select i, b, c from t1) as m");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(101, resultSet.getInt(1));
                    Assertions.assertEquals(30, resultSet.getInt(2));
                    Assertions.assertEquals(500, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(102, resultSet.getInt(1));
                    Assertions.assertEquals(34, resultSet.getInt(2));
                    Assertions.assertEquals(501, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest6() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select * from (select pk, pk from t1) as m options ( nocache )");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(42, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(43, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest7() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select ratings, loc.longitude from (select i.* from t1) as m options ( nocache )");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(30, resultSet.getInt(1));
                    Assertions.assertEquals(5000, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(34, resultSet.getInt(1));
                    Assertions.assertEquals(5001, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void starExpansionTest8() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info array, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, [(30, (5000, 5500))], 500, 101), (43, [(34, (5001, 5501))], 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select latitude, longitude from (select ratings, loc.* from t1, t1.i) as m");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(5500, resultSet.getInt(1));
                    Assertions.assertEquals(5000, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(5501, resultSet.getInt(1));
                    Assertions.assertEquals(5001, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Disabled // TODO (Expand nested struct fields)
    @Test
    void starExpansionTestDisabled() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Info (ratings bigint, nested1 bigint, nested2 bigint) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, 5000, 5500), 500, 101), (43, (34, 5001, 5501), 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select t1.i.* from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(30, resultSet.getInt(1));
                    Assertions.assertEquals(5000, resultSet.getInt(2));
                    Assertions.assertEquals(5500, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(34, resultSet.getInt(1));
                    Assertions.assertEquals(5001, resultSet.getInt(2));
                    Assertions.assertEquals(5501, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void orderByTest1() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info array, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                "create index i1 as select pk, i from t2 order by pk, i";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, [(30, (5000, 5500))], 500, 101), (43, [(34, (5001, 5501))], 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select pk, i from t2 order by pk, i");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(400, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(600, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void orderByTest2() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info array, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                "create index i1 as select pk, i from t2 order by pk, i";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, [(30, (5000, 5500))], 500, 101), (43, [(34, (5001, 5501))], 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                relationalStatement.executeInternal("select pk, b from t2 order by pk, i options ( nocache )");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(500, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(400, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void orderByTest3() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info array, b bigint, c bigint, PRIMARY KEY(pk))" +
                "CREATE TABLE T2(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                "create index i1 as select pk, i from t2 order by pk, i";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, [(30, (5000, 5500))], 500, 101), (43, [(34, (5001, 5501))], 501, 102)");
                relationalStatement.executeInternal("insert into t2 values (42, 400, 500), (43, 600, 400)");
                Assertions.assertThrows(RelationalException.class, () -> relationalStatement.executeInternal("select pk, b from (select * from t2 order by pk, i) as x"));
            }
        }
    }

    @Test
    void aggregateFunctionTest1() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                relationalStatement.executeInternal("select count(pk), i, b from t1 group by i, b");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertEquals(13, resultSet.getInt(2));
                    Assertions.assertEquals(18, resultSet.getInt(3));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertEquals(15, resultSet.getInt(2));
                    Assertions.assertEquals(20, resultSet.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void aggregateFunctionTest2() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                relationalStatement.executeInternal("select count(pk), i + b from t1 group by i, b");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertEquals(31, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertEquals(35, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void aggregateFunctionTest3SelectStar() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                RelationalAssertions.assertThrows(() ->
                        ((EmbeddedRelationalStatement) statement)
                                .executeInternal("select * from t1 group by i"))
                        .hasErrorCode(ErrorCode.GROUPING_ERROR);
            }
        }
    }

    @Test
    void simpleArithmeticExpression1() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                relationalStatement.executeInternal("select b + 10, 100 - i from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(28, resultSet.getInt(1));
                    Assertions.assertEquals(87, resultSet.getInt(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(30, resultSet.getInt(1));
                    Assertions.assertEquals(85, resultSet.getInt(2));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate1() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                relationalStatement.executeInternal("select i from t1 where b > 18");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(15, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate2InPredicate() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                relationalStatement.executeInternal("select i from t1 where b in (18, 200)");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(13, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate3InPredicate() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b bigint, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 18), (43, 15, 20)");
                relationalStatement.executeInternal("select i from t1 where not b in (18, 200)");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(15, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate4LikePredicate() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, 'hello'), (43, 15, 'world')");
                relationalStatement.executeInternal("select i from t1 where b like '%ello'");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(13, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate5LikePredicateWithEscape() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, '___abcdef'), (43, 15, 'world')");
                relationalStatement.executeInternal("select i from t1 where b like '\\_\\_\\_abcdef' escape '\\'");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(13, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate6IsNull() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, null), (43, 15, 'world')");
                relationalStatement.executeInternal("select i from t1 where b is null");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(13, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate6IsNotNull() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, null), (43, 15, 'world')");
                relationalStatement.executeInternal("select i from t1 where b is not null");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(15, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simplePredicate7ExistsPredicate() throws Exception {
        final String schemaTemplate =
                "        create table a(ida integer, x integer, primary key(ida))\n" +
                        "        create table x(idx integer, y integer, primary key(idx))\n" +
                        "        CREATE TYPE AS STRUCT s(f integer)\n" +
                        "        create table r(idr integer, nr s array, primary key(idr))\n" +
                        "        create table b(idb integer, q integer, r integer, primary key(idb))\n" +
                        "        create index ib as select q from b\n" +
                        "        create index ir as select sq.f from r, (select f from r.nr) sq;";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into A values (1, 1), (2, 2), (3, 3)");
                relationalStatement.executeInternal("insert into X values (4, 10), (5, 20), (6, 30)");
                relationalStatement.executeInternal("insert into R values (1, [(11), (12), (13)]), (2, [(21), (22), (23)]), (3, [(31), (32), (33)])");
                relationalStatement.executeInternal("insert into B values (1, 10, 100), (2, 20, 200), (3, 30, 300)");

                relationalStatement.executeInternal("select ida from a where exists (select ida from a where ida = 1)");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(2, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select idx from x where exists (select x from a where ida = 1)");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(4, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(5, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(6, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select idr from r, r.nr as NEST where NEST.f = 23");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(2, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select x from a where exists (select a.x, max(idb) from b where q > a.x group by q)");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(2, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select x from a where exists (select x, max(idb) from b where q > x group by q)");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(2, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select x from a where exists (select max(x) from b where q > x group by q)");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(2, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select x from a where exists (select max(a.x), max(idb) from b where q > x group by q)");
                try (final RelationalResultSet resultSet = relationalStatement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(1, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(2, resultSet.getInt(1));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void simpleInsert1() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, null), (43, 15, 'world')");
                relationalStatement.executeInternal("select * from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(13, resultSet.getInt(2));
                    resultSet.getInt(3);
                    Assertions.assertTrue(resultSet.wasNull());
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(15, resultSet.getInt(2));
                    Assertions.assertEquals("world", resultSet.getString(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void visitSelectWithLimit() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, null), (43, 15, 'world')");
                relationalStatement.executeInternal("select * from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(13, resultSet.getInt(2));
                    resultSet.getInt(3);
                    Assertions.assertTrue(resultSet.wasNull());
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(43, resultSet.getInt(1));
                    Assertions.assertEquals(15, resultSet.getInt(2));
                    Assertions.assertEquals("world", resultSet.getString(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    // add tests for EXPLAIN.

    @Test
    void updateStatement1() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, null), (43, 15, 'world')");
                relationalStatement.executeInternal("update t1 set i = i + 10 where pk = 42");
                relationalStatement.executeInternal("select * from t1 where pk = 42");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(23, resultSet.getInt(2));
                    resultSet.getInt(3);
                    Assertions.assertTrue(resultSet.wasNull());
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void updateStatement3SetQualifiedField() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, i bigint, b string, PRIMARY KEY(pk))" +
                        "create index i1 as select i, b from t1 order by i, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, 13, null), (43, 15, 'world')");
                relationalStatement.executeInternal("update t1 set t1.i = t1.i + 10 where pk = 42");
                relationalStatement.executeInternal("select * from t1 where pk = 42");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(23, resultSet.getInt(2));
                    resultSet.getInt(3);
                    Assertions.assertTrue(resultSet.wasNull());
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void updateStatement4SetNestedField() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("update t1 set i.loc.longitude = 8888 where pk = 42");
                relationalStatement.executeInternal("select * from t1 where pk = 42");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(30, resultSet.getStruct("i").getLong("ratings"));
                    Assertions.assertEquals(8888, resultSet.getStruct("i").getStruct("loc").getLong("longitude"));
                    Assertions.assertEquals(5500, resultSet.getStruct("i").getStruct("loc").getLong("latitude"));
                    Assertions.assertEquals(500, resultSet.getInt(3));
                    Assertions.assertEquals(101, resultSet.getInt(4));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void testNestedValues() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT N (O BIGINT, P BIGINT)" +
                "                      CREATE TYPE AS STRUCT M (n N)" +
                "                      CREATE TYPE AS STRUCT C (m M)" +
                "                      CREATE TYPE AS STRUCT B (c C)" +
                "                      CREATE TYPE AS STRUCT A (b B) " +
                "CREATE TABLE T1(pk bigint, a A, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (((((34, 35))))))");
                relationalStatement.executeInternal("select x.c.m.n.p from (select a.b.c from t1) as x");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(35, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }

                relationalStatement.executeInternal("select c.m.n.p from (select a.b.c from t1) as x");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(35, resultSet.getInt(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void deleteStatement1() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT Location (longitude bigint, latitude bigint) " +
                "CREATE TYPE AS STRUCT Info (ratings bigint, loc Location) " +
                "CREATE TABLE T1(pk bigint, i Info, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var relationalStatement = (EmbeddedRelationalStatement) statement;
                relationalStatement.executeInternal("insert into t1 values (42, (30, (5000, 5500)), 500, 101), (43, (34, (5001, 5501)), 501, 102)");
                relationalStatement.executeInternal("delete from t1 where i.loc.longitude = 5001");
                relationalStatement.executeInternal("select * from t1");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(42, resultSet.getInt(1));
                    Assertions.assertEquals(30, resultSet.getStruct("i").getLong("ratings"));
                    Assertions.assertEquals(5000, resultSet.getStruct("i").getStruct("loc").getLong("longitude"));
                    Assertions.assertEquals(5500, resultSet.getStruct("i").getStruct("loc").getLong("latitude"));
                    Assertions.assertEquals(500, resultSet.getInt(3));
                    Assertions.assertEquals(101, resultSet.getInt(4));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }
}
