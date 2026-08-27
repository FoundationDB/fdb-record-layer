/*
 * StoredQuerySignatureTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.StoredQuery;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Map;

/**
 * The signature of a stored query: how its parameters are declared, what is persisted for them, and how references to
 * them in the body are turned into the {@code ?name} form a prepared statement uses.
 *
 * <p>These tests stop at the metadata. Planning a stored query from its signature is exercised separately.</p>
 */
public class StoredQuerySignatureTest {

    private static final String TABLE = "CREATE TABLE t1(id bigint, col1 bigint, col2 string, PRIMARY KEY(id))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    /**
     * Runs {@code template} as a schema template and hands the resulting metadata to the caller.
     */
    private RecordLayerSchemaTemplate templateOf(final String dbName, final String template) throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create(dbName))
                .relationalExtension(relationalExtension)
                .schemaTemplate(template)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection().unwrap(EmbeddedRelationalConnection.class);
            connection.setAutoCommit(false);
            connection.createNewTransaction();
            final var schemaTemplate = connection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            connection.rollback();
            connection.setAutoCommit(true);
            return schemaTemplate;
        }
    }

    private Map<String, StoredQuery> storedQueriesOf(final String dbName, final String template) throws Exception {
        return templateOf(dbName, template).getStoredQueries();
    }

    private void expectFailure(final String dbName, final String template, final String messageFragment) {
        RelationalAssertions.assertThrowsSqlException(() ->
                        Ddl.builder()
                                .database(URI.create(dbName))
                                .relationalExtension(relationalExtension)
                                .schemaTemplate(template)
                                .build())
                .hasErrorCode(ErrorCode.UNSUPPORTED_QUERY)
                .hasMessageContaining(messageFragment);
    }

    /**
     * A parameter name is an ordinary identifier: unquoted it is uppercased, quoted it keeps its spelling. That
     * normalized name is what is persisted, and it is the name a client has to bind, because a prepared parameter name
     * is never normalized.
     */
    @Test
    void declarationIsPersistedUnderTheNormalizedName() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_NAMES", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT, \"mixedCase\" STRING NOT NULL)"
                + " AS SELECT id FROM t1 WHERE col1 = param_a AND col2 = \"mixedCase\"");
        Assertions.assertThat(storedQueries.get("Q").getParameters())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "PARAM_A", "BIGINT",
                        "mixedCase", "STRING NOT NULL"));
    }

    /**
     * The declaration is kept as source text, whitespace and all, so it can be parsed back. Rebuilding it from tokens
     * would turn {@code BIGINT ARRAY} into {@code BIGINTARRAY}.
     */
    @Test
    void declarationTextKeepsItsSpacing() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_TEXT", TABLE
                + " CREATE STORED QUERY q(p1 BIGINT ARRAY, p2 BIGINT NOT NULL, p3 BIGINT NULL)"
                + " AS SELECT id FROM t1 WHERE col1 = p2");
        Assertions.assertThat(storedQueries.get("Q").getParameters())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "P1", "BIGINT ARRAY",
                        "P2", "BIGINT NOT NULL",
                        "P3", "BIGINT NULL"));
    }

    /**
     * A reference in the body becomes {@code ?name}, using the normalized spelling — which is what makes a quoted
     * declaration reachable by a client that sends a mixed-case parameter name.
     */
    @Test
    void bodyReferencesBecomeNamedParameters() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_REWRITE", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT, \"mixedCase\" STRING)"
                + " AS SELECT id FROM t1 WHERE col1 = param_a AND col2 = \"mixedCase\"");
        Assertions.assertThat(storedQueries.get("Q").getQuery())
                .isEqualTo("SELECT id FROM t1 WHERE col1 = ?PARAM_A AND col2 = ?mixedCase");
    }

    /**
     * A qualified reference is a column, not a parameter, even when a parameter of that name exists — a signature
     * parameter never has a qualifier.
     */
    @Test
    void qualifiedReferenceIsLeftAlone() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_QUALIFIED", TABLE
                + " CREATE STORED QUERY q(col1 BIGINT)"
                + " AS SELECT id FROM t1 WHERE t1.col1 = 10");
        Assertions.assertThat(storedQueries.get("Q").getQuery())
                .isEqualTo("SELECT id FROM t1 WHERE t1.col1 = 10");
    }

    /**
     * A parameter may be captured by a declared function's body, and the rewrite reaches into it. The function's own
     * parameters are untouched.
     */
    @Test
    void referencesInsideDeclaredFunctionsAreRewritten() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_FUNC", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT)"
                + " DECLARE FUNCTION f1(p BIGINT) AS (SELECT * FROM t1 WHERE col1 = p AND col2 = param_a)"
                + " AS SELECT id FROM f1(param_a)");
        final var storedQuery = storedQueries.get("Q");
        Assertions.assertThat(storedQuery.getQuery()).isEqualTo("SELECT id FROM f1(?PARAM_A)");
        Assertions.assertThat(storedQuery.getTempFunctions()).hasSize(1);
        Assertions.assertThat(storedQuery.getTempFunctions().get(0))
                .contains("f1(p BIGINT)")
                .contains("col1 = p AND col2 = ?PARAM_A");
    }

    /**
     * A query with no signature keeps the behaviour it had before signatures existed: nothing declared, body verbatim.
     */
    @Test
    void queryWithoutSignatureIsUnchanged() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_NONE", TABLE
                + " CREATE STORED QUERY q AS SELECT id FROM t1 WHERE col1 = 10");
        Assertions.assertThat(storedQueries.get("Q").getParameters()).isEmpty();
        Assertions.assertThat(storedQueries.get("Q").getQuery()).isEqualTo("SELECT id FROM t1 WHERE col1 = 10");
    }

    /**
     * A signature has to survive the metadata it is stored in: the declarations are written into
     * {@code PStoredQueryParameter} and read back out, so a warm-up that happens in a later process sees exactly what
     * {@code CREATE} recorded.
     */
    @Test
    void signatureSurvivesAProtoRoundTrip() throws Exception {
        final var template = templateOf("/TEST/SQS_ROUNDTRIP", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT, \"mixedCase\" STRING NOT NULL, p3 BIGINT ARRAY)"
                + " DECLARE FUNCTION f1(p BIGINT) AS (SELECT * FROM t1 WHERE col1 = p AND col2 = \"mixedCase\")"
                + " AS SELECT id FROM f1(param_a)");

        final var rebuilt = RecordLayerSchemaTemplate.fromRecordMetadata(
                RecordMetaData.build(template.toRecordMetadata().toProto()),
                template.getName(), template.getVersion());

        final var before = template.getStoredQueries().get("Q");
        final var after = rebuilt.getStoredQueries().get("Q");
        Assertions.assertThat(after.getParameters()).isEqualTo(before.getParameters());
        Assertions.assertThat(after.getParameters())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "PARAM_A", "BIGINT",
                        "mixedCase", "STRING NOT NULL",
                        "P3", "BIGINT ARRAY"));
        Assertions.assertThat(after.getQuery()).isEqualTo(before.getQuery());
        Assertions.assertThat(after.getTempFunctions()).isEqualTo(before.getTempFunctions());
    }

    /**
     * Two parameters that normalize to the same identifier are the same parameter, so the second is a mistake rather
     * than a redefinition.
     */
    @Test
    void duplicateParameterIsRejected() {
        expectFailure("/TEST/SQS_DUP", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT, PARAM_A BIGINT)"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a",
                "duplicate stored query signature parameter");
    }

    /**
     * A signature parameter and one of a declared function's own parameters naming the same identifier are
     * indistinguishable inside that function's body, so the rewrite would capture the wrong one.
     */
    @Test
    void parameterCollidingWithDeclaredFunctionParameterIsRejected() {
        expectFailure("/TEST/SQS_SHADOW", TABLE
                        + " CREATE STORED QUERY q(p BIGINT)"
                        + " DECLARE FUNCTION f1(p BIGINT) AS (SELECT * FROM t1 WHERE col1 = p)"
                        + " AS SELECT id FROM f1(p)",
                "collides with a stored query signature parameter");
    }
}
