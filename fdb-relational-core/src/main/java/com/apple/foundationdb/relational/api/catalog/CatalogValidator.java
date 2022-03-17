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

import com.apple.foundationdb.relational.api.CatalogData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;

public class CatalogValidator {
    public static void validateSchema(@Nonnull CatalogData.Schema schema) throws RelationalException {
        // fields schema_name, schema_version, schema_template_name, database_id are required
        if (schema.getSchemaName().isEmpty()) {
            throw new RelationalException("Field schema_name in Schema must be set!", ErrorCode.INVALID_PARAMETER);
        }
        if (schema.getDatabaseId().isEmpty()) {
            throw new RelationalException("Field database_id in Schema must be set!", ErrorCode.INVALID_PARAMETER);
        }
        if (schema.getSchemaTemplateName().isEmpty()) {
            throw new RelationalException("Field schema_template_name in Schema must be set!", ErrorCode.INVALID_PARAMETER);
        }
        // if not set, default value for int fields is 0
        if (schema.getSchemaVersion() == 0) {
            throw new RelationalException("Field schema_version in Schema must be set, and must not be 0!", ErrorCode.INVALID_PARAMETER);
        }
        // validate tables field
        for (CatalogData.Table t : schema.getTablesList()) {
            validateTable(t);
        }
    }

    public static void validateTable(@Nonnull CatalogData.Table table) throws RelationalException {
        // field name is required
        if (table.getName().isEmpty()) {
            throw new RelationalException("Field name in Table must be set!", ErrorCode.INVALID_PARAMETER);
        }
    }
}
