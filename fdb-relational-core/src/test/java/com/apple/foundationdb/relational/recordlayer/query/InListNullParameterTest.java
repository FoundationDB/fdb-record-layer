/*
 * InListNullParameterTest.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.Types;

/**
 * An IN list holding a named parameter that is bound to NULL. This is the shape a client sends, since a client binds
 * values rather than writing literals, so it matters more in practice than a NULL written into the query text.
 * <br>
 * A list holding a parameter is not an all-constant list — the grammar lists a prepared parameter as its own
 * expression atom — so it is built by the array constructor at run time rather than coalesced into one array literal.
 * The element only turns out to be null while the query runs, which used to reach an {@code ImmutableList} and come
 * back as a bare {@code NullPointerException}.
 */
public class InListNullParameterTest {

    private static final String SCHEMA_TEMPLATE = "CREATE TABLE T(id bigint, name string, PRIMARY KEY(id))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void nullBoundIntoAnInListIsReported() throws Exception {
        try (var ddl = ddl()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT id FROM T WHERE id IN (?p)")) {
                statement.setNull("p", Types.BIGINT);
                RelationalAssertions.assertThrowsSqlException(statement::executeQuery)
                        .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION)
                        .hasMessageContaining("NULL is not allowed as an array element");
            }
        }
    }

    @Test
    void nullBoundBesideALiteralIsReported() throws Exception {
        try (var ddl = ddl()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT id FROM T WHERE id IN (1, ?p)")) {
                statement.setNull("p", Types.BIGINT);
                RelationalAssertions.assertThrowsSqlException(statement::executeQuery)
                        .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION)
                        .hasMessageContaining("NULL is not allowed as an array element");
            }
        }
    }

    /**
     * The same query with a value bound, so that the check above is known to be about the NULL and not about the shape
     * of the query.
     */
    @Test
    void valueBoundIntoAnInListWorks() throws Exception {
        try (var ddl = ddl()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT id FROM T WHERE id IN (?p)")) {
                statement.setLong("p", 2L);
                try (var resultSet = statement.executeQuery()) {
                    Assertions.assertThat(resultSet.next()).isTrue();
                    Assertions.assertThat(resultSet.getLong(1)).isEqualTo(2L);
                    Assertions.assertThat(resultSet.next()).isFalse();
                }
            }
        }
    }

    private Ddl ddl() throws Exception {
        final var ddl = Ddl.builder().database(URI.create("/TEST/PN"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build();
        try (var insert = ddl.setSchemaAndGetConnection().createStatement()) {
            insert.executeUpdate("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
        }
        return ddl;
    }
}
