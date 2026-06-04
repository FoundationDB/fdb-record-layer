/*
 * ThrowingMetadataOperationsFactoryTest.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThrowingMetadataOperationsFactoryTest {

    private final MetadataOperationsFactory factory = ThrowingMetadataOperationsFactory.INSTANCE;
    private final URI dbUri = URI.create("/FRL/DB");
    private final SchemaTemplate template = Mockito.mock(SchemaTemplate.class);
    private final RecordLayerInvokedRoutine routine = Mockito.mock(RecordLayerInvokedRoutine.class);

    @Test
    void saveSchemaTemplateThrows() {
        assertRejected(() -> factory.getSaveSchemaTemplateConstantAction(template, Options.NONE), "CREATE SCHEMA TEMPLATE");
    }

    @Test
    void dropSchemaTemplateThrows() {
        assertRejected(() -> factory.getDropSchemaTemplateConstantAction("tmpl", true, Options.NONE), "DROP SCHEMA TEMPLATE");
    }

    @Test
    void createDatabaseThrows() {
        assertRejected(() -> factory.getCreateDatabaseConstantAction(dbUri, Options.NONE), "CREATE DATABASE");
    }

    @Test
    void createSchemaThrows() {
        assertRejected(() -> factory.getCreateSchemaConstantAction(dbUri, "schema", "tmpl", Options.NONE), "CREATE SCHEMA");
    }

    @Test
    void dropDatabaseThrows() {
        assertRejected(() -> factory.getDropDatabaseConstantAction(dbUri, true, Options.NONE), "DROP DATABASE");
    }

    @Test
    void dropSchemaThrows() {
        assertRejected(() -> factory.getDropSchemaConstantAction(dbUri, "schema", Options.NONE), "DROP SCHEMA");
    }

    @Test
    void createTemporaryFunctionThrows() {
        assertRejected(() -> factory.getCreateTemporaryFunctionConstantAction(template, true, routine), "CREATE TEMPORARY FUNCTION");
    }

    @Test
    void dropTemporaryFunctionThrows() {
        assertRejected(() -> factory.getDropTemporaryFunctionConstantAction(true, "fn"), "DROP TEMPORARY FUNCTION");
    }

    private static void assertRejected(Runnable action, String operation) {
        assertThatThrownBy(action::run)
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("'" + operation + "'")
                .extracting(t -> ((UncheckedRelationalException) t).unwrap().getErrorCode())
                .isEqualTo(ErrorCode.UNSUPPORTED_OPERATION);
    }
}
