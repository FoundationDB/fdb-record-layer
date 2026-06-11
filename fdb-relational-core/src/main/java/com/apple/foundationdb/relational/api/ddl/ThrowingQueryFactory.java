/*
 * ThrowingQueryFactory.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * A {@link DdlQueryFactory} that rejects every administrative metadata query by throwing
 * an {@link com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException}
 * at factory-lookup time. Use this in DQL-only contexts (e.g. offline planning) where any
 * DDL query should fail fast.
 */
@API(API.Status.EXPERIMENTAL)
public final class ThrowingQueryFactory implements DdlQueryFactory {
    public static final ThrowingQueryFactory INSTANCE = new ThrowingQueryFactory();

    private ThrowingQueryFactory() {
    }

    private static RuntimeException reject(@Nonnull final String operation) {
        return new RelationalException(
                "DDL query '" + operation + "' is not allowed in this context",
                ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
    }

    @Override
    public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
        throw reject("SHOW DATABASES");
    }

    @Override
    public DdlQuery getListSchemaTemplatesQueryAction() {
        throw reject("SHOW SCHEMA TEMPLATES");
    }

    @Override
    public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
        throw reject("DESCRIBE SCHEMA TEMPLATE");
    }

    @Override
    public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbId, @Nonnull String schemaId) {
        throw reject("DESCRIBE SCHEMA");
    }
}
