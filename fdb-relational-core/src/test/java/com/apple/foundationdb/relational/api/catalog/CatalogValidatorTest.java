/*
 * CatalogValidatorTest.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatalogValidatorTest {
    @Test
    void testValidateTableWithUnsetName() {
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateTable(generateTableWithUnsetName());
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field name in Table must be set!", exception.getMessage());
    }

    @Test
    void testValidateGoodTable() {
        Assertions.assertDoesNotThrow(() -> {
            CatalogValidator.validateTable(generateGoodTable());
        });
    }

    @Test
    void testValidateGoodSchema() {
        Assertions.assertDoesNotThrow(() -> {
            CatalogValidator.validateSchema(generateGoodSchema());
        });
    }

    @Test
    void testValidateWithUnsetSchemaName() {
        CatalogData.Schema goodSchema = generateGoodSchema();
        // clear schema_name field
        CatalogData.Schema schemaWithUnsetSchemaName = goodSchema.toBuilder().clearSchemaName().build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(schemaWithUnsetSchemaName);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_name in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetDatabaseId() {
        CatalogData.Schema goodSchema = generateGoodSchema();
        // clear database_id field
        CatalogData.Schema badSchema = goodSchema.toBuilder().clearDatabaseId().build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field database_id in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetTemplateName() {
        CatalogData.Schema goodSchema = generateGoodSchema();
        // clear schema_template_name field
        CatalogData.Schema badSchema = goodSchema.toBuilder().clearSchemaTemplateName().build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_template_name in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetVersion() {
        CatalogData.Schema goodSchema = generateGoodSchema();
        // clear schema_version field
        CatalogData.Schema badSchema = goodSchema.toBuilder().clearSchemaVersion().build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_version in Schema must be set, and must not be 0!", exception.getMessage());
    }

    @Test
    void testValidateWithBadTable() {
        CatalogData.Schema goodSchema = generateGoodSchema();
        // clear schema_version field
        CatalogData.Schema badSchema = goodSchema.toBuilder().addTables(generateTableWithUnsetName()).build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field name in Table must be set!", exception.getMessage());
    }

    private CatalogData.Table generateGoodTable() {
        return CatalogData.Table.newBuilder().setName("table_name1").build();
    }

    private CatalogData.Table generateTableWithUnsetName() {
        return CatalogData.Table.getDefaultInstance();
    }

    private CatalogData.Schema generateGoodSchema() {
        return CatalogData.Schema.newBuilder()
                .setSchemaName("test_schema_name")
                .setDatabaseId("test_database_id")
                .setSchemaTemplateName("test_schema_template_name")
                .setSchemaVersion(1)
                .addTables(generateGoodTable())
                .build();
    }
}
