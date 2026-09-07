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
import com.apple.foundationdb.relational.api.metadata.StoredQuery.ParameterState;
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
import java.util.List;
import java.util.Map;

/**
 * The signature of a stored query and the {@code PREPARE FOR} block that goes with it: how parameters are declared,
 * which combinations of their states are warmed, what is persisted for both, and how references to a parameter in the
 * body are turned into the {@code ?name} form a prepared statement uses.
 *
 * <p>A signature and a {@code PREPARE FOR} block require each other, so they are tested together. Every case pins every
 * declared parameter, which is what keeps a parameter from being planned with no value and a nullable type at once —
 * such a plan is not correct for a null binding.</p>
 *
 * <p>These tests stop at the metadata. Planning a stored query from its signature is exercised separately.</p>
 */
public class StoredQuerySignatureTest {

    private static final String TABLE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 string, flag boolean, PRIMARY KEY(id))";

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
     * is never normalized. A case names its parameters the same way.
     */
    @Test
    void declarationIsPersistedUnderTheNormalizedName() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_NAMES", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT, \"mixedCase\" STRING NOT NULL)"
                + " PREPARE FOR ((param_a IS NOT NULL, \"mixedCase\" IS NOT NULL))"
                + " AS SELECT id FROM t1 WHERE col1 = param_a AND col2 = \"mixedCase\"");
        Assertions.assertThat(storedQueries.get("Q").getParameters())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "PARAM_A", "BIGINT",
                        "mixedCase", "STRING NOT NULL"));
        Assertions.assertThat(storedQueries.get("Q").getPreparedCases())
                .containsExactly(Map.of(
                        "PARAM_A", ParameterState.IS_NOT_NULL,
                        "mixedCase", ParameterState.IS_NOT_NULL));
    }

    /**
     * The declaration is kept as source text, whitespace and all, so it can be parsed back. Rebuilding it from tokens
     * would turn {@code BIGINT ARRAY} into {@code BIGINTARRAY}.
     */
    @Test
    void declarationTextKeepsItsSpacing() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_TEXT", TABLE
                + " CREATE STORED QUERY q(p1 BIGINT ARRAY, p2 BIGINT NOT NULL, p3 BIGINT NULL)"
                + " PREPARE FOR ((p1 IS NOT NULL, p2 IS NOT NULL, p3 IS NULL))"
                + " AS SELECT id FROM t1 WHERE col1 = p2");
        Assertions.assertThat(storedQueries.get("Q").getParameters())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "P1", "BIGINT ARRAY",
                        "P2", "BIGINT NOT NULL",
                        "P3", "BIGINT NULL"));
    }

    /**
     * Each written state is persisted as the constant naming it. The four are the only ones that change a plan rather
     * than only its constraints.
     */
    @Test
    void everyStateIsPersisted() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_STATES", TABLE
                + " CREATE STORED QUERY q(p1 BIGINT, p2 BIGINT, b BOOLEAN)"
                + " PREPARE FOR ((p1 IS NULL, p2 IS NOT NULL, b = TRUE),"
                + "              (p1 IS NULL, p2 IS NOT NULL, b = FALSE))"
                + " AS SELECT id FROM t1 WHERE col1 = p1 AND col2 = p2 AND flag = b");
        Assertions.assertThat(storedQueries.get("Q").getPreparedCases()).containsExactly(
                Map.of("P1", ParameterState.IS_NULL, "P2", ParameterState.IS_NOT_NULL, "B", ParameterState.IS_TRUE),
                Map.of("P1", ParameterState.IS_NULL, "P2", ParameterState.IS_NOT_NULL, "B", ParameterState.IS_FALSE));
    }

    /**
     * A reference in the body becomes {@code ?name}, using the normalized spelling — which is what makes a quoted
     * declaration reachable by a client that sends a mixed-case parameter name.
     */
    @Test
    void bodyReferencesBecomeNamedParameters() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_REWRITE", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT, \"mixedCase\" STRING)"
                + " PREPARE FOR ((param_a IS NOT NULL, \"mixedCase\" IS NOT NULL))"
                + " AS SELECT id FROM t1 WHERE col1 = param_a AND col2 = \"mixedCase\"");
        Assertions.assertThat(storedQueries.get("Q").getQuery())
                .isEqualTo("SELECT id FROM t1 WHERE col1 = ?PARAM_A AND col2 = ?mixedCase");
    }

    /**
     * A {@code PREPARE FOR} block is not part of the query, so it leaves no trace in the stored body.
     */
    @Test
    void preparedCasesAreNotPartOfTheStoredBody() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_BODY", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT)"
                + " PREPARE FOR ((param_a IS NULL), (param_a IS NOT NULL))"
                + " AS SELECT id FROM t1 WHERE col1 = param_a");
        Assertions.assertThat(storedQueries.get("Q").getQuery())
                .isEqualTo("SELECT id FROM t1 WHERE col1 = ?PARAM_A");
    }

    /**
     * A qualified reference is a column, not a parameter, even when a parameter of that name exists — a signature
     * parameter never has a qualifier.
     */
    @Test
    void qualifiedReferenceIsLeftAlone() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_QUALIFIED", TABLE
                + " CREATE STORED QUERY q(col1 BIGINT)"
                + " PREPARE FOR ((col1 IS NOT NULL))"
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
                + " PREPARE FOR ((param_a IS NOT NULL))"
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
     * A query with no signature keeps the behaviour it had before signatures existed: nothing declared, no cases, body
     * verbatim. With no parameters there is nothing to pin, so no {@code PREPARE FOR} is required.
     */
    @Test
    void queryWithoutSignatureIsUnchanged() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_NONE", TABLE
                + " CREATE STORED QUERY q AS SELECT id FROM t1 WHERE col1 = 10");
        Assertions.assertThat(storedQueries.get("Q").getParameters()).isEmpty();
        Assertions.assertThat(storedQueries.get("Q").getPreparedCases()).isEmpty();
        Assertions.assertThat(storedQueries.get("Q").getQuery()).isEqualTo("SELECT id FROM t1 WHERE col1 = 10");
    }

    /**
     * A signature and its cases have to survive the metadata they are stored in: they are written into
     * {@code PStoredQueryParameter} and {@code PPreparedCase} and read back out, so a warm-up that happens in a later
     * process sees exactly what {@code CREATE} recorded. The states go through the wire as canonical tokens, so this
     * also pins that mapping in both directions.
     */
    @Test
    void signatureAndCasesSurviveAProtoRoundTrip() throws Exception {
        final var template = templateOf("/TEST/SQS_ROUNDTRIP", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT, \"mixedCase\" STRING NOT NULL, p3 BIGINT ARRAY, b BOOLEAN)"
                + " PREPARE FOR ((param_a IS NULL, \"mixedCase\" IS NOT NULL, p3 IS NOT NULL, b = TRUE),"
                + "              (param_a IS NOT NULL, \"mixedCase\" IS NOT NULL, p3 IS NOT NULL, b = FALSE))"
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
                        "P3", "BIGINT ARRAY",
                        "B", "BOOLEAN"));
        Assertions.assertThat(after.getPreparedCases()).isEqualTo(before.getPreparedCases());
        Assertions.assertThat(after.getPreparedCases()).containsExactly(
                Map.of("PARAM_A", ParameterState.IS_NULL, "mixedCase", ParameterState.IS_NOT_NULL,
                        "P3", ParameterState.IS_NOT_NULL, "B", ParameterState.IS_TRUE),
                Map.of("PARAM_A", ParameterState.IS_NOT_NULL, "mixedCase", ParameterState.IS_NOT_NULL,
                        "P3", ParameterState.IS_NOT_NULL, "B", ParameterState.IS_FALSE));
        Assertions.assertThat(after.getQuery()).isEqualTo(before.getQuery());
        Assertions.assertThat(after.getTempFunctions()).isEqualTo(before.getTempFunctions());
    }

    /**
     * Two parameters that normalize to the same identifier are the same parameter, so the second is a mistake rather
     * than a redefinition. This is caught while reading the signature, before the cases are looked at, which is why the
     * statement needs no {@code PREPARE FOR} to reach it.
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
                        + " PREPARE FOR ((p IS NOT NULL))"
                        + " DECLARE FUNCTION f1(p BIGINT) AS (SELECT * FROM t1 WHERE col1 = p)"
                        + " AS SELECT id FROM f1(p)",
                "collides with a stored query signature parameter");
    }

    /**
     * Declaring parameters and leaving the cases out would mean warming a plan for a parameter that has neither a value
     * nor a non-nullable type, and no single plan is correct for both a null and a non-null binding.
     */
    @Test
    void signatureWithoutPreparedCasesIsRejected() {
        expectFailure("/TEST/SQS_NOCASES", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT)"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a",
                "requires a PREPARE FOR block");
    }

    /**
     * The other direction: a block with nothing to pin.
     */
    @Test
    void preparedCasesWithoutSignatureAreRejected() {
        expectFailure("/TEST/SQS_NOSIG", TABLE
                        + " CREATE STORED QUERY q"
                        + " PREPARE FOR ((param_a IS NOT NULL))"
                        + " AS SELECT id FROM t1 WHERE col1 = 10",
                "PREPARE FOR requires a signature");
    }

    @Test
    void unknownParameterInACaseIsRejected() {
        expectFailure("/TEST/SQS_UNKNOWN", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT)"
                        + " PREPARE FOR ((param_a IS NOT NULL, param_b IS NOT NULL))"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a",
                "which the signature does not declare");
    }

    /**
     * A case that leaves a parameter out is the state this whole block exists to prevent.
     */
    @Test
    void incompleteCaseIsRejected() {
        expectFailure("/TEST/SQS_INCOMPLETE", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT, param_b BIGINT)"
                        + " PREPARE FOR ((param_a IS NOT NULL))"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a AND col2 = param_b",
                "every case must pin every declared parameter");
    }

    @Test
    void parameterNamedTwiceInOneCaseIsRejected() {
        expectFailure("/TEST/SQS_CASEDUP", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT)"
                        + " PREPARE FOR ((param_a IS NULL, param_a IS NOT NULL))"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a",
                "more than once");
    }

    /**
     * A {@code NOT NULL} declaration says the parameter never receives a null, so warming the null case for it
     * contradicts the declaration rather than refining it.
     */
    @Test
    void nullCaseForNotNullParameterIsRejected() {
        expectFailure("/TEST/SQS_NOTNULL", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT NOT NULL)"
                        + " PREPARE FOR ((param_a IS NULL))"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a",
                "but the signature declares it NOT NULL");
    }

    /**
     * {@code = TRUE} pins a value, so it only means something for a parameter that can hold that value. {@code BOOLEAN}
     * is a primitive, so this is decided from the declaration itself and needs no type resolution.
     */
    @Test
    void booleanStateForNonBooleanParameterIsRejected() {
        expectFailure("/TEST/SQS_NOTBOOL", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT)"
                        + " PREPARE FOR ((param_a = TRUE))"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a",
                "does not declare it BOOLEAN");
    }

    /**
     * An array of booleans is not a boolean either.
     */
    @Test
    void booleanStateForBooleanArrayParameterIsRejected() {
        expectFailure("/TEST/SQS_BOOLARRAY", TABLE
                        + " CREATE STORED QUERY q(flags BOOLEAN ARRAY)"
                        + " PREPARE FOR ((flags = TRUE))"
                        + " AS SELECT id FROM t1 WHERE col1 = 10",
                "does not declare it BOOLEAN");
    }

    /**
     * Two cases pinning the same parameters to the same states are one case however they are written, and warming both
     * would build the same plan twice.
     */
    @Test
    void duplicateCaseIsRejected() {
        expectFailure("/TEST/SQS_DUPCASE", TABLE
                        + " CREATE STORED QUERY q(param_a BIGINT, param_b BIGINT)"
                        + " PREPARE FOR ((param_a IS NULL, param_b IS NOT NULL),"
                        + "              (param_b IS NOT NULL, param_a IS NULL))"
                        + " AS SELECT id FROM t1 WHERE col1 = param_a AND col2 = param_b",
                "duplicate prepared case");
    }

    /**
     * Cases that differ are kept in the order written, which is the order they are warmed in.
     */
    @Test
    void distinctCasesAreKeptInOrder() throws Exception {
        final var storedQueries = storedQueriesOf("/TEST/SQS_ORDER", TABLE
                + " CREATE STORED QUERY q(param_a BIGINT)"
                + " PREPARE FOR ((param_a IS NOT NULL), (param_a IS NULL))"
                + " AS SELECT id FROM t1 WHERE col1 = param_a");
        Assertions.assertThat(storedQueries.get("Q").getPreparedCases()).containsExactly(
                Map.of("PARAM_A", ParameterState.IS_NOT_NULL),
                Map.of("PARAM_A", ParameterState.IS_NULL));
    }

    /**
     * The record layer keeps the states as canonical tokens, since it never interprets them. This pins the tokens
     * themselves, which a rename would silently change for every stored query already persisted.
     */
    @Test
    void recordLayerKeepsStatesAsCanonicalTokens() throws Exception {
        final var recordMetaData = templateOf("/TEST/SQS_TOKENS", TABLE
                + " CREATE STORED QUERY q(p1 BIGINT, b BOOLEAN)"
                + " PREPARE FOR ((p1 IS NULL, b = FALSE))"
                + " AS SELECT id FROM t1 WHERE col1 = p1 AND flag = b").toRecordMetadata();
        Assertions.assertThat(recordMetaData.getStoredQueries().get("Q").getPreparedCases())
                .isEqualTo(List.of(Map.of("P1", "IS_NULL", "B", "IS_FALSE")));
    }
}
