/*
 * RecordLayerStoreCatalogImplTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.SchemaExistsBehavior;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.test.ParameterizedTestUtils;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RecordLayerStoreCatalogImplTest extends RecordLayerStoreCatalogTestBase {

    /** Initial template name used by tests involving {@link SecondSaveShape}. **/
    private static final String INITIAL_TEMPLATE = "tmpl";
    /** Initial template version used by tests involving {@link SecondSaveShape}. **/
    private static final int INITIAL_VERSION = 1;

    @BeforeEach
    void setUpCatalog() throws RelationalException {
        fdb = FDBDatabaseFactory.instance().getDatabase(FDBTestEnvironment.randomClusterFile());
        // create a FDBRecordStore
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        }
    }

    @AfterEach
    void deleteAllRecords() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {

            final KeySpacePath keySpacePath = RelationalKeyspaceProvider.toDatabasePath(URI.create("/__SYS"), keySpace).schemaPath("CATALOG");
            FDBRecordStore.deleteStoreAsync(txn.unwrap(FDBRecordContext.class), keySpacePath).join();
            txn.commit();
        }
    }

    @Test
    void testLoadSchema() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion, true);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false, SchemaExistsBehavior.ERROR);
            txn.commit();
        }

        // test loadSchema method with correct schema name
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result = storeCatalog.loadSchema(loadTxn1, URI.create("/TEST/test_database_id"), "test_schema_name");
            Assertions.assertEquals("test_schema_name", result.getName());
            Assertions.assertEquals("test_template_name", result.getSchemaTemplate().getName());
            Assertions.assertEquals(1, result.getSchemaTemplate().getVersion());
            Assertions.assertEquals(2, result.getTables().size());
            assertThat(result.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
            assertThat(result.getViews().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_view1", "test_view2");
            assertThat(result.getViews().stream().map(View::getDescription).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("select * from test_table1", "select * from test_table2 where A = 'foo'");
        }
    }

    @Test
    void testSaveSchemaWithoutTemplate() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            final var thrown = Assertions.assertThrows(RelationalException.class, () -> storeCatalog.saveSchema(txn, schema1, false, SchemaExistsBehavior.ERROR));
            Assertions.assertEquals("Cannot create schema test_schema_name because schema template test_template_name version 1 does not exist.",
                    thrown.getMessage());
            Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE, thrown.getErrorCode());
            txn.commit();
        }
    }

    @Test
    void testLoadSchemaWithoutTemplate() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        // save record in FDB
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false, SchemaExistsBehavior.ERROR);
            txn.commit();
        }

        // delete the schema template.
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().deleteTemplate(txn, schema1.getSchemaTemplate().getName(), true);
            txn.commit();
        }

        // test loadSchema method with correct schema name
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn1, URI.create("/TEST/test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE, exception.getErrorCode());
        }
    }

    @Test
    void testRepairSchema() throws RelationalException {
        // save schema with template version 1L
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false, SchemaExistsBehavior.ERROR);
            txn.commit();
        }
        // save schema template with version 2
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 2);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, template2);
            txn.commit();
        }
        // repair schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.repairSchema(txn, "/TEST/test_database_id", "test_schema_name");
            txn.commit();
        }
        // load schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema newSchema = storeCatalog.loadSchema(txn, URI.create("/TEST/test_database_id"), "test_schema_name");
            txn.commit();
            // template version should be the latest version
            Assertions.assertEquals(2L, newSchema.getSchemaTemplate().getVersion());
        }
    }

    @Test
    void loadSchemaFailsWithNonexistentDatabase() throws RelationalException {
        // test loadSchema method with a nonexistent database_id
        try (Transaction loadTxn2 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn2, URI.create("/TEST/test_wrong_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA, exception.getErrorCode());
        }
    }

    @Test
    void loadSchemaFailsWithNOnExistentSchemaName() throws RelationalException {
        // test loadSchema method with a nonexistent schema
        try (Transaction loadTxn3 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn3, URI.create("/TEST/test_database_id"), "test_wrong_schema_name"));
            Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA, exception2.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithCommittedTransaction() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            txn.commit();
            RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(txn, URI.create("/TEST/test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithAbortedTransaction() throws RelationalException {
        // abort
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            txn2.abort();
            RelationalException exception4 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(txn2, URI.create("/TEST/test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception4.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithClosedTransaction() throws RelationalException {
        // close
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception5 = Assertions.assertThrows(RelationalException.class, () ->
                storeCatalog.loadSchema(txn3, URI.create("/TEST/test_database_id"), "test_schema_name"));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception5.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithCommittedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            // committed
            txn1.commit();
            RelationalException exception1 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.saveSchema(txn1, schema1, false, SchemaExistsBehavior.ERROR));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception1.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithAbortedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            // aborted
            txn2.abort();
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.saveSchema(txn2, schema1, false, SchemaExistsBehavior.ERROR));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception2.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithClosedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () ->
                storeCatalog.saveSchema(txn3, schema1, false, SchemaExistsBehavior.ERROR));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithTwoConsecutiveTransactions() throws RelationalException {
        // 2 schemas with different versions
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        final Schema schema2 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 2);
        final SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 1, true);
        final SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 2, true);
        // test 2 successful consecutive transactions
        // update with schema1 (version = 1)
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn1, template1);
            storeCatalog.createDatabase(txn1, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn1, schema1, false, SchemaExistsBehavior.ERROR);
            // commit and close the write transaction
            txn1.commit();
        }
        // read after 1st transaction commit
        try (Transaction readTransaction1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result1 = storeCatalog.loadSchema(readTransaction1, URI.create("/TEST/test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertEquals("test_schema_name", result1.getName());
            Assertions.assertEquals("test_template_name", result1.getSchemaTemplate().getName());
            Assertions.assertEquals(1, result1.getSchemaTemplate().getVersion());

            assertThat(result1.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
            assertThat(result1.getViews().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_view1", "test_view2");
            assertThat(result1.getViews().stream().map(View::getDescription).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("select * from test_table1", "select * from test_table2 where A = 'foo'");
        }
        // update with schema2 (version = 2)
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn2, template2);
            storeCatalog.saveSchema(txn2, schema2, false, SchemaExistsBehavior.UPGRADE);
            txn2.commit();
        }

        // read after 2nd transaction
        try (Transaction readTransaction2 = new RecordContextTransaction(fdb.openContext())) {
            Schema result2 = storeCatalog.loadSchema(readTransaction2, URI.create("/TEST/test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertEquals("test_schema_name", result2.getName());
            Assertions.assertEquals("test_template_name", result2.getSchemaTemplate().getName());
            Assertions.assertEquals(2, result2.getSchemaTemplate().getVersion());

            assertThat(result2.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
            assertThat(result2.getViews().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_view1", "test_view2");
            assertThat(result2.getViews().stream().map(View::getDescription).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("select * from test_table1", "select * from test_table2 where A = 'foo'");
        }
    }

    @Test
    void testUpdateSchemaWithTwoSimultaneousTransactions() throws RelationalException {
        // 2 schemas with different versions
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        final Schema schema2 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 2);

        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            txn.commit();
        }

        // test 2 conflicting transactions
        try (Transaction txn3 = new RecordContextTransaction(fdb.openContext()); Transaction txn4 = new RecordContextTransaction(fdb.openContext())) {
            // update with 2 different schemas
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn3, schema1.getSchemaTemplate());
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn4, schema2.getSchemaTemplate());
            storeCatalog.saveSchema(txn3, schema1, false, SchemaExistsBehavior.ERROR);
            storeCatalog.saveSchema(txn4, schema2, false, SchemaExistsBehavior.ERROR);
            // commit the first write transaction
            txn3.commit();
            // assert that the second transaction couldn't be committed
            assertThatThrownBy(txn4::commit)
                    .isInstanceOf(RelationalException.class)
                    .extracting("errorCode")
                    .isEqualTo(ErrorCode.SERIALIZATION_FAILURE);
        }
    }

    @Test
    void testUpdateSchemaWithBadSchema() throws RelationalException {
        // bad schema, schema_version must not be negative
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", -34);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.saveSchema(txn, schema1, false, SchemaExistsBehavior.ERROR));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
            Assertions.assertEquals("Field schema_version cannot be < 0!", exception.getMessage());
        }
    }

    @Test
    void testTwoSimultaneousInitializationsDoNotConflict() throws RelationalException {
        // With the catalog schema already present from @BeforeEach, two concurrent invocations of
        // StoreCatalogProvider.getCatalog (which each call initialize -> saveSchema for the
        // hard-coded catalog schema) must not race each other: both should observe the existing
        // catalog row, skip the redundant write, and commit as read-only transactions. Prior to
        // the idempotency check in initialize, both transactions wrote to the same primary key
        // and the second commit failed with SERIALIZATION_FAILURE.
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext());
                Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            StoreCatalogProvider.getCatalog(txn1, keySpace);
            StoreCatalogProvider.getCatalog(txn2, keySpace);
            txn1.commit();
            txn2.commit();
        }

        // The catalog schema should still be present.
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertTrue(storeCatalog.doesSchemaExist(txn, URI.create("/__SYS"), "CATALOG"));
        }
    }

    @Test
    void testCreateSchemaWithSchemaTemplateVersionZero() throws RelationalException {
        // bad schema, schema_version must not be negative
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 0);
        final SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 0);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, template1);
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false, SchemaExistsBehavior.ERROR);
            txn.commit();
        }

        // read after transaction commit
        try (Transaction readTransaction1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result1 = storeCatalog.loadSchema(readTransaction1, URI.create(schema1.getDatabaseName()), schema1.getName());
            // Assert template version is 0
            Assertions.assertEquals(0, result1.getSchemaTemplate().getVersion());
        }
    }

    /** How the second saveSchema call's schema relates to the already-committed one. */
    private enum SecondSaveShape {
        /** Same (templateName, templateVersion) as the existing row. */
        IDENTICAL,
        /** Same template name, strictly-newer version. */
        NEWER_VERSION,
        /** Same template name, strictly-older version. */
        OLDER_VERSION,
        /** Different template name (versions irrelevant). */
        DIFFERENT_TEMPLATE_NAME;

        /**
         * Returns {@code true} iff a {@code saveSchema} call with this shape and the given
         * behavior returns normally (i.e. does not throw {@code SCHEMA_ALREADY_EXISTS}).
         */
        public boolean succeeds(final SchemaExistsBehavior behavior) {
            return switch (behavior) {
                // ERROR always throws when a schema is already present, regardless of shape.
                case ERROR -> false;
                // Only an identical schema is silently accepted; anything else throws.
                case ERROR_IF_DIFFERENT -> this == IDENTICAL;
                // DO_NOTHING is a silent no-op for every shape.
                case DO_NOTHING -> true;
                // UPGRADE accepts an identical (no-op) or strictly-newer (write) schema;
                // an older version or different template name throws.
                case UPGRADE -> this == IDENTICAL || this == NEWER_VERSION;
            };
        }

        /**
         * Returns {@code true} iff a successful {@code saveSchema} call with this shape and the
         * given behavior actually writes the schema row (as opposed to silently returning
         * without touching it). Implies {@link #succeeds(SchemaExistsBehavior)}.
         */
        public boolean doesWrite(final SchemaExistsBehavior behavior) {
            // The only branch that ever writes on the exists path is UPGRADE with a strictly-newer
            // template version. Everything else either throws (see #succeeds) or is a no-op.
            return behavior == SchemaExistsBehavior.UPGRADE && this == NEWER_VERSION;
        }
    }

    /**
     * Commit a schema with {@link #INITIAL_TEMPLATE} at {@link #INITIAL_VERSION} for the given database.
     * Also, this pre-registers the templates that follow-up saves might reference so those saves aren't rejected by
     * the template-existence assertion. Returns the database URI so callers can address the
     * schema.
     */
    @Nonnull
    private URI preSaveExistingSchema(@Nonnull String dbSuffix) throws RelationalException {
        final String dbId = "/TEST/" + dbSuffix;
        final Schema existing = generateTestSchema("s", dbId, INITIAL_TEMPLATE, INITIAL_VERSION);
        final SchemaTemplate newerVersion = generateTestSchemaTemplate(INITIAL_TEMPLATE, INITIAL_VERSION + 1);
        final SchemaTemplate olderVersion = generateTestSchemaTemplate(INITIAL_TEMPLATE, INITIAL_VERSION - 1);
        final SchemaTemplate differentName = generateTestSchemaTemplate(INITIAL_TEMPLATE + "-other", INITIAL_VERSION);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, existing.getSchemaTemplate());
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, newerVersion);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, olderVersion);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, differentName);
            storeCatalog.createDatabase(txn, URI.create(dbId));
            storeCatalog.saveSchema(txn, existing, false, SchemaExistsBehavior.ERROR);
            txn.commit();
        }
        return URI.create(dbId);
    }

    /** Build the schema used for the second save based on {@code shape}. */
    @Nonnull
    private Schema secondSaveSchema(@Nonnull URI dbId, @Nonnull SecondSaveShape shape) {
        return switch (shape) {
            case IDENTICAL -> generateTestSchema("s", dbId.toString(), INITIAL_TEMPLATE, INITIAL_VERSION);
            case NEWER_VERSION -> generateTestSchema("s", dbId.toString(), INITIAL_TEMPLATE, INITIAL_VERSION + 1);
            case OLDER_VERSION -> generateTestSchema("s", dbId.toString(), INITIAL_TEMPLATE, INITIAL_VERSION - 1);
            case DIFFERENT_TEMPLATE_NAME ->
                    generateTestSchema("s", dbId.toString(), INITIAL_TEMPLATE + "-other", INITIAL_VERSION);
        };
    }

    /** Reload the schema at {@code (dbId, "s")} and assert the persisted template matches. */
    private void assertPersistedTemplateEquals(@Nonnull URI dbId, @Nonnull String expectedTemplateName,
                                               int expectedVersion) throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final Schema reloaded = storeCatalog.loadSchema(txn, dbId, "s");
            Assertions.assertEquals(expectedTemplateName, reloaded.getSchemaTemplate().getName());
            Assertions.assertEquals(expectedVersion, reloaded.getSchemaTemplate().getVersion());
        }
    }

    static Stream<Arguments> saveSchemaExistsBehavior() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(SecondSaveShape.values()),
                Stream.of(SchemaExistsBehavior.values()));
    }

    /** Test a saveSchema with various exists behaviors when the schema already exists. **/
    @ParameterizedTest
    @MethodSource
    void saveSchemaExistsBehavior(@Nonnull SecondSaveShape shape, @Nonnull SchemaExistsBehavior behavior) throws RelationalException {
        URI dbId = preSaveExistingSchema("db_error_" + shape.name().toLowerCase(Locale.ROOT));
        final Schema second = secondSaveSchema(dbId, shape);
        if (shape.succeeds(behavior)) {
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                storeCatalog.saveSchema(txn, second, false, behavior);
                txn.commit();
            }
        } else {
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                RelationalAssertions.assertThrows(
                        () -> storeCatalog.saveSchema(txn, second, false, behavior))
                        .hasErrorCode(ErrorCode.SCHEMA_ALREADY_EXISTS);
            }
        }
        if (shape.doesWrite(behavior)) {
            assertPersistedTemplateEquals(dbId, second.getSchemaTemplate().getName(),
                    second.getSchemaTemplate().getVersion());
        } else {
            assertPersistedTemplateEquals(dbId, INITIAL_TEMPLATE, INITIAL_VERSION);
        }
    }

    /** Test a saveSchema with various exists behaviors when the schema does not already exist. **/
    @ParameterizedTest
    @EnumSource(SchemaExistsBehavior.class)
    void saveSchemaExistsBehaviorWithNothing(@Nonnull SchemaExistsBehavior behavior) throws RelationalException {
        final String dbId = "/TEST/" + "schema_exists_with_nothing" + behavior;
        final Schema initialSchema = generateTestSchema("s", dbId, INITIAL_TEMPLATE, INITIAL_VERSION);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, initialSchema.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(dbId));
            storeCatalog.saveSchema(txn, initialSchema, false, behavior);
            txn.commit();
        }
        assertPersistedTemplateEquals(URI.create(dbId), INITIAL_TEMPLATE, INITIAL_VERSION);
    }

    public static Stream<Arguments> concurrentSaveSchema() {
        // ERROR always throws synchronously when the schema already exists, so it can never
        // participate in the "both saveSchema calls returned; then we commit" scenario we're
        // exercising. Skip it in both slots.
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(SchemaExistsBehavior.values()).filter(behavior -> behavior != SchemaExistsBehavior.ERROR),
                Stream.of(SchemaExistsBehavior.values()).filter(behavior -> behavior != SchemaExistsBehavior.ERROR),
                Stream.of(SecondSaveShape.values()),
                Stream.of(SecondSaveShape.values()),
                ParameterizedTestUtils.booleans("txn2UnrelatedWrite")
        );
    }

    /**
     * Direct concurrent-{@code saveSchema} coverage across every non-throwing (shape, behavior)
     * pair the two transactions might see, optionally with an unrelated write in {@code txn2}.
     *
     * <p>{@link #testTwoSimultaneousInitializationsDoNotConflict()} exercises a subset of this
     * same path via {@code StoreCatalogProvider.getCatalog}; this test additionally covers every
     * synchronous-succeed combination on each side.</p>
     */
    @ParameterizedTest
    @MethodSource
    void concurrentSaveSchema(@Nonnull SchemaExistsBehavior behavior1,
                              @Nonnull SchemaExistsBehavior behavior2,
                              @Nonnull SecondSaveShape shape1,
                              @Nonnull SecondSaveShape shape2,
                              boolean txn2UnrelatedWrite) throws RelationalException {
        // We only exercise pairs where BOTH sides return from saveSchema — otherwise the
        // synchronous throw kills the setup and there's no commit ordering to observe.
        Assumptions.assumeThat(shape1.succeeds(behavior1)).isTrue();
        Assumptions.assumeThat(shape2.succeeds(behavior2)).isTrue();

        final URI dbId = preSaveExistingSchema("db_concurrent_" +
                String.join("_", behavior1.name(), behavior2.name(),
                        shape1.name(), shape2.name(), Boolean.toString(txn2UnrelatedWrite)));

        final Schema secondForTxn1 = secondSaveSchema(dbId, shape1);
        final Schema secondForTxn2 = secondSaveSchema(dbId, shape2);
        final boolean txn1Writes = shape1.doesWrite(behavior1);
        final boolean txn2Writes = shape2.doesWrite(behavior2);
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext());
                Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.saveSchema(txn1, secondForTxn1, false, behavior1);
            storeCatalog.saveSchema(txn2, secondForTxn2, false, behavior2);
            if (txn2UnrelatedWrite) {
                // Do an unrelated write in txn2, otherwise txn2 is read-only
                storeCatalog.createDatabase(txn2, URI.create("/TEST/" + UUID.randomUUID()));
            }
            txn1.commit();
            final boolean txn2ReachesResolver = txn2Writes || txn2UnrelatedWrite;
            if (txn1Writes && txn2ReachesResolver) {
                // Read-write conflict on the schema row: txn1's committed write invalidates
                // txn2's earlier read of that row, and txn2's commit reaches the resolver
                // (either via its own schema write or via the unrelated write above).
                RelationalAssertions.assertThrows(txn2::commit)
                                .hasErrorCode(ErrorCode.SERIALIZATION_FAILURE);
            } else {
                // Either txn1 didn't write, or txn2 is truly read-only and skips the resolver.
                // Both commit cleanly.
                txn2.commit();
            }
        }

        // Post-state depends on which committed writes happened:
        //   * txn1 wrote → on-disk = txn1's write (whether txn2 aborted or committed as a
        //     no-op on the schema row, the schema row now reflects txn1)
        //   * only txn2 wrote → on-disk = txn2's write
        //   * neither wrote → on-disk unchanged
        if (txn1Writes) {
            assertPersistedTemplateEquals(dbId, secondForTxn1.getSchemaTemplate().getName(),
                    secondForTxn1.getSchemaTemplate().getVersion());
        } else if (txn2Writes) {
            assertPersistedTemplateEquals(dbId, secondForTxn2.getSchemaTemplate().getName(),
                    secondForTxn2.getSchemaTemplate().getVersion());
        } else {
            assertPersistedTemplateEquals(dbId, INITIAL_TEMPLATE, INITIAL_VERSION);
        }
    }
}
