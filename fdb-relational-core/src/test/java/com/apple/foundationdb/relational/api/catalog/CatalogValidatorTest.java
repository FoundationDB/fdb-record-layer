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
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

public class CatalogValidatorTest {
    @Test
    void testValidateGoodSchema() {
        Assertions.assertDoesNotThrow(() -> CatalogValidator.validateSchema(generateGoodSchema()));
    }

    @Test
    void testValidateWithUnsetRecordLayerSchemaName() {
        RecordLayerSchema goodSchema = generateGoodSchema();
        // clear schema_name field
        RecordLayerSchema schemaWithUnsetRecordLayerSchemaName = (RecordLayerSchema) goodSchema.getSchemaTemplate().generateSchema(goodSchema.getDatabaseName(), null);
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                CatalogValidator.validateSchema(schemaWithUnsetRecordLayerSchemaName));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_name in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetDatabaseId() {
        RecordLayerSchema goodSchema = generateGoodSchema();
        // clear database_id field
        RecordLayerSchema badRecordLayerSchema = (RecordLayerSchema) goodSchema.getSchemaTemplate().generateSchema(null, goodSchema.getName());

        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badRecordLayerSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field database_id in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetTemplateName() throws RelationalException {
        // clear schema_template_name field
        RecordLayerSchema badRecordLayerSchema = (RecordLayerSchema) generateBadSchemaWithEmptySchemaTemplateName().getSchemaTemplate().generateSchema("foo", "bar");
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badRecordLayerSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_template_name in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetVersion() throws RelationalException {
        // clear schema_version field
        RecordLayerSchema badRecordLayerSchema = (RecordLayerSchema) generateBadSchemaWithWrongVersion().getSchemaTemplate().generateSchema("foo", "bar");
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badRecordLayerSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_version in Schema must be set, and must be > 0!", exception.getMessage());
    }

    @Nonnull
    private RecordLayerSchema generateGoodSchema() {
        return RecordLayerSchemaTemplate
                .newBuilder()
                .addTable(
                        RecordLayerTable
                                .newBuilder()
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("A")
                                                .setDataType(DataType.Primitives.STRING.type())
                                                .build())
                                .setName("test_table")
                                .addPrimaryKeyPart(List.of("A"))
                                .build())
                .setVersion(1L)
                .setName("test_template")
                .build()
                .generateSchema("test_db", "test_schema");
    }

    @Nonnull
    private RecordLayerSchema generateBadSchemaWithWrongVersion() {
        return RecordLayerSchemaTemplate
                .newBuilder()
                .addTable(
                        RecordLayerTable
                                .newBuilder()
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("A")
                                                .setDataType(DataType.Primitives.STRING.type())
                                                .build())
                                .setName("test_table")
                                .addPrimaryKeyPart(List.of("A"))
                                .build())
                .setVersion(-42L)
                .setName("test_template")
                .build()
                .generateSchema("test_db", "test_schema");
    }

    @Nonnull
    private RecordLayerSchema generateBadSchemaWithEmptySchemaTemplateName() {
        return RecordLayerSchemaTemplate
                .newBuilder()
                .addTable(
                        RecordLayerTable
                                .newBuilder()
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("A")
                                                .setDataType(DataType.Primitives.STRING.type())
                                                .build())
                                .setName("test_table")
                                .addPrimaryKeyPart(List.of("A"))
                                .build())
                .setVersion(1L)
                .setName("")
                .build()
                .generateSchema("test_db", "test_schema");
    }
}
