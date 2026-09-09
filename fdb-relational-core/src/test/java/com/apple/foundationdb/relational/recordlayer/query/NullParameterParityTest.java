/*
 * NullParameterParityTest.java
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

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.Types;

/**
 * A NULL in the query text and a parameter bound with {@code setNull} mean the same SQL value. But they reach the
 * planner as different nodes:
 * <ul>
 *     <li>a text NULL becomes a {@code NullValue} ({@code ExpressionVisitor.visitNullLiteral})</li>
 *     <li>a bound NULL becomes a {@code ConstantObjectValue} of NULL type
 *     ({@code MutablePlanGenerationContext.processNamedPreparedParam}, via {@code Type.fromObject(null)})</li>
 * </ul>
 * Code that looks for a NULL by node class, instead of by type, sees the first shape but not the second. Clients that
 * bind their NULLs then get different behaviour from the same query.
 */
public class NullParameterParityTest {

    private static final String SCHEMA_TEMPLATE = "CREATE TABLE T(id bigint, name string, PRIMARY KEY(id))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    /**
     * A bare NULL as the whole WHERE clause. NULL matches nothing, so the query returns no rows. A bound NULL must do
     * the same. Before the fix in {@code Expression.Utils.toUnderlyingPredicate}, the bound form failed with
     * "expected boolean expression but got NULL".
     */
    @Test
    void boundNullAsTopLevelPredicateBehavesLikeInlineNull() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            try (var insert = ddl.setSchemaAndGetConnection().createStatement()) {
                Assertions.assertThat(insert.executeUpdate("INSERT INTO T VALUES (1, 'a')")).isEqualTo(1);
            }

            // NULL in the query text.
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT id FROM T WHERE NULL")) {
                try (final RelationalResultSet resultSet = statement.executeQuery()) {
                    Assertions.assertThat(resultSet.next()).describedAs("WHERE NULL matches nothing").isFalse();
                }
            }

            // The same query with the NULL bound instead.
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("SELECT id FROM T WHERE ?p")) {
                statement.setNull("p", Types.NULL);
                try (final RelationalResultSet resultSet = statement.executeQuery()) {
                    Assertions.assertThat(resultSet.next()).describedAs("WHERE ?p bound to NULL matches nothing").isFalse();
                }
            }
        }
    }
}
