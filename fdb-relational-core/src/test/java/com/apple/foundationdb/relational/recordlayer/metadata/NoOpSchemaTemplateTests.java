/*
 * NoOpSchemaTemplateTest.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link NoOpSchemaTemplate}.
 */
public class NoOpSchemaTemplateTests {

    @Test
    public void testConstructorAndGetters() {
        final String templateName = "test_template";
        final int version = 1;

        final NoOpSchemaTemplate template = new NoOpSchemaTemplate(templateName, version);

        assertEquals(templateName, template.getName());
        assertEquals(version, template.getVersion());
    }

    @Test
    public void testIsEnableLongRows() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);
        assertFalse(template.isEnableLongRows());
    }

    @Test
    public void testIsStoreRowVersions() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);
        assertFalse(template.isStoreRowVersions());
    }

    @Test
    public void testGenerateSchema() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test_template", 2);
        final String databaseId = "db123";
        final String schemaName = "my_schema";

        final Schema schema = template.generateSchema(databaseId, schemaName);

        assertNotNull(schema);
        assertInstanceOf(RecordLayerSchema.class, schema);
        assertEquals(schemaName, schema.getName());
    }

    @Test
    public void testGetTablesThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getTables);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have tables!", exception.getMessage());
    }

    @Test
    public void testGetViewsThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getViews);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have views!", exception.getMessage());
    }

    @Test
    public void testFindTableByNameThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                () -> template.findTableByName("some_table"));

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have tables!", exception.getMessage());
    }

    @Test
    public void testFindViewByNameThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                () -> template.findViewByName("some_view"));

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have views!", exception.getMessage());
    }

    @Test
    public void testGetTableIndexMappingThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getTableIndexMapping);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have indexes!", exception.getMessage());
    }

    @Test
    public void testGetIndexesThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getIndexes);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have indexes!", exception.getMessage());
    }

    @Test
    public void testGetIndexEntriesAsBitsetThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                () -> template.getIndexEntriesAsBitset(Optional.empty()));

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have indexes!", exception.getMessage());
    }

    @Test
    public void testGetInvokedRoutinesThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getInvokedRoutines);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have invoked routines!", exception.getMessage());
    }

    @Test
    public void testFindInvokedRoutineByNameThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                () -> template.findInvokedRoutineByName("some_routine"));

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have invoked routines!", exception.getMessage());
    }

    @Test
    public void testGetTemporaryInvokedRoutinesThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getTemporaryInvokedRoutines);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have temporary invoked routines!", exception.getMessage());
    }

    @Test
    public void testGetTransactionBoundMetadataAsStringThrowsException() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final RelationalException exception = assertThrows(RelationalException.class,
                template::getTransactionBoundMetadataAsString);

        assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        assertEquals("NoOpSchemaTemplate doesn't have temporary invoked routines!", exception.getMessage());
    }

    @Test
    public void testUnwrap() throws RelationalException {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final NoOpSchemaTemplate unwrapped = template.unwrap(NoOpSchemaTemplate.class);

        assertEquals(template, unwrapped);
    }

    @Test
    public void testMultipleVersions() {
        final NoOpSchemaTemplate v1 = new NoOpSchemaTemplate("template", 1);
        final NoOpSchemaTemplate v2 = new NoOpSchemaTemplate("template", 2);
        final NoOpSchemaTemplate v100 = new NoOpSchemaTemplate("template", 100);

        assertEquals(1, v1.getVersion());
        assertEquals(2, v2.getVersion());
        assertEquals(100, v100.getVersion());
    }

    @Test
    public void testDifferentNames() {
        final NoOpSchemaTemplate template1 = new NoOpSchemaTemplate("schema_a", 1);
        final NoOpSchemaTemplate template2 = new NoOpSchemaTemplate("schema_b", 1);

        assertEquals("schema_a", template1.getName());
        assertEquals("schema_b", template2.getName());
    }

    @Test
    public void testGenerateSchemaWithDifferentParameters() {
        final NoOpSchemaTemplate template = new NoOpSchemaTemplate("test", 1);

        final Schema schema1 = template.generateSchema("db1", "schema1");
        final Schema schema2 = template.generateSchema("db2", "schema2");

        assertEquals("schema1", schema1.getName());
        assertEquals("schema2", schema2.getName());
    }
}
