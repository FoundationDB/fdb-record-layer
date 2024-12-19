/*
 * CatalogValidator.java
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
import com.apple.foundationdb.relational.api.metadata.Schema;

import javax.annotation.Nonnull;

public final class CatalogValidator {

    // this seems superfluous, it can be replaced with proper checks in {@link Schema} constructor.
    public static void validateSchema(@Nonnull Schema schema) throws RelationalException {
        // fields schema_name, schema_version, schema_template_name, database_id are required
        if (schema.getName() == null || schema.getName().isEmpty()) {
            throw new RelationalException("Field schema_name in Schema must be set!", ErrorCode.INVALID_PARAMETER);
        }
        if (schema.getDatabaseName() == null || schema.getDatabaseName().isEmpty()) {
            throw new RelationalException("Field database_id in Schema must be set!", ErrorCode.INVALID_PARAMETER);
        }
        if (schema.getSchemaTemplate().getName() == null || schema.getSchemaTemplate().getName().isEmpty()) {
            throw new RelationalException("Field schema_template_name in Schema must be set!", ErrorCode.INVALID_PARAMETER);
        }
        // Schema version cannot be negative ever. However, it can be zero in some cases.
        if (schema.getSchemaTemplate().getVersion() < 0) {
            throw new RelationalException("Field schema_version cannot be < 0!", ErrorCode.INVALID_PARAMETER);
        }
        // We are assuming that the RecordMetaData field has been validated by RecordLayer

    }

    private CatalogValidator() {
    }
}
