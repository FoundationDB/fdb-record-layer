/*
 * SetTransactionVariableTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

/**
 * Tests for the {@code SET TRANSACTION VARIABLE} statement. There is no SQL-level way to read a
 * variable's value yet (that's {@code GET_VARIABLE}, added in the next stacked PR), so these
 * assert directly on {@link EmbeddedRelationalConnection#getTransaction()} instead.
 */
public class SetTransactionVariableTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void setStoresTheValueInTheTransaction() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/STV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 42");
                // Unquoted identifiers fold to upper case by default, and a bare decimal literal
                // parses as an Integer (not Long) unless coerced by a typed column comparison.
                Assertions.assertThat(conn.unwrap(EmbeddedRelationalConnection.class).getTransaction().getLocalVariables())
                        .containsEntry("X", 42);
            }
            conn.rollback();
        }
    }

    @Test
    void overwritingAVariableReplacesThePreviousValue() throws Exception {
        final String schemaTemplate = "create table t2(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/STV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 1");
                stmt.execute("set transaction variable x = 2");
                Assertions.assertThat(conn.unwrap(EmbeddedRelationalConnection.class).getTransaction().getLocalVariables())
                        .containsEntry("X", 2);
            }
            conn.rollback();
        }
    }

    @Test
    void nullIsALegitimateValue() throws Exception {
        final String schemaTemplate = "create table t3(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/STV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = null");
                final var localVars = conn.unwrap(EmbeddedRelationalConnection.class).getTransaction().getLocalVariables();
                Assertions.assertThat(localVars).containsKey("X");
                Assertions.assertThat(localVars.get("X")).isNull();
            }
            conn.rollback();
        }
    }

    @Test
    void quotedVariableNamePreservesCase() throws Exception {
        final String schemaTemplate = "create table t4(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/STV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable \"myVar\" = 10");
                Assertions.assertThat(conn.unwrap(EmbeddedRelationalConnection.class).getTransaction().getLocalVariables())
                        .containsEntry("myVar", 10);
            }
            conn.rollback();
        }
    }

    @Test
    void variableNotVisibleInNextTransactionAfterCommit() throws Exception {
        final String schemaTemplate = "create table t5(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/STV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 42");
            }
            conn.commit();

            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            Assertions.assertThat(conn.unwrap(EmbeddedRelationalConnection.class).getTransaction().getLocalVariables())
                    .doesNotContainKey("x");
            conn.rollback();
        }
    }

    @Test
    void variableNotVisibleAfterRollback() throws Exception {
        final String schemaTemplate = "create table t6(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/STV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 42");
            }
            conn.rollback();

            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            Assertions.assertThat(conn.unwrap(EmbeddedRelationalConnection.class).getTransaction().getLocalVariables())
                    .doesNotContainKey("x");
            conn.rollback();
        }
    }
}
