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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.Schema;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CatalogValidatorTest {
    @Test
    void testValidateGoodSchema() {
        Assertions.assertDoesNotThrow(() -> CatalogValidator.validateSchema(generateGoodSchema()));
    }

    @Test
    void testValidateWithUnsetSchemaName() throws RelationalException {
        Schema goodSchema = generateGoodSchema();
        // clear schema_name field
        Schema schemaWithUnsetSchemaName = new Schema(goodSchema.getDatabaseId(), null,
                goodSchema.getMetaData(), goodSchema.getSchemaTemplateName(), goodSchema.getTemplateVersion());
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                CatalogValidator.validateSchema(schemaWithUnsetSchemaName));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_name in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetDatabaseId() throws RelationalException {
        Schema goodSchema = generateGoodSchema();
        // clear database_id field
        Schema badSchema = new Schema(null, goodSchema.getSchemaName(),
                goodSchema.getMetaData(), goodSchema.getSchemaTemplateName(), goodSchema.getTemplateVersion());

        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field database_id in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetTemplateName() throws RelationalException {
        Schema goodSchema = generateGoodSchema();
        // clear schema_template_name field
        Schema badSchema = new Schema(goodSchema.getDatabaseId(), goodSchema.getSchemaName(),
                goodSchema.getMetaData(), null, goodSchema.getTemplateVersion());
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_template_name in Schema must be set!", exception.getMessage());
    }

    @Test
    void testValidateWithUnsetVersion() throws RelationalException {
        Schema goodSchema = generateGoodSchema();
        // clear schema_version field
        Schema badSchema = new Schema(goodSchema.getDatabaseId(), goodSchema.getSchemaName(),
                goodSchema.getMetaData(), goodSchema.getSchemaTemplateName(), 0);
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            CatalogValidator.validateSchema(badSchema);
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
        Assertions.assertEquals("Field schema_version in Schema must be set, and must be > 0!", exception.getMessage());
    }

    private Schema generateGoodSchema() throws RelationalException {
        TypingContext ctx = TypingContext.create();

        TypingContext.FieldDefinition aField = new TypingContext.FieldDefinition("A", Type.TypeCode.STRING, null, false);
        ctx.addType(new TypingContext.TypeDefinition("test_table", List.of(aField), true, List.of(List.of("A"))));

        ctx.addAllToTypeRepository();
        final SchemaTemplate template = ctx.generateSchemaTemplate("test_template", 1L);
        return template.generateSchema("test_db", "test_schema");
    }
}
