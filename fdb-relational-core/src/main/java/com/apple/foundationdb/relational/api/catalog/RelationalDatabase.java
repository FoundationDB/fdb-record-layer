/*
 * RelationalDatabase.java
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

import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;

/**
 * There can only be 1 Database object per Connection instance, and it's lifecycle is managed by the connection
 * itself.
 */
@ConnectionScoped
public interface RelationalDatabase extends AutoCloseable {

    /**
     * To force verification that the schema exists, set {@link OperationOption#forceVerifyDdl()}.
     *
     * @param schemaId the unique id of the schema
     * @param options  the options for loading.
     * @return a Schema for the specified id.
     * @throws RelationalException if something goes wrong during load, or if {@link OperationOption#forceVerifyDdl()}
     *                           is true and the Schema does not exist. If the schema does not exist and {@link OperationOption#forceVerifyDdl()}
     *                           is present in the options, then an error is thrown with {@link ErrorCode#UNKNOWN_SCHEMA}
     */
    DatabaseSchema loadSchema(@Nonnull String schemaId, @Nonnull Options options) throws RelationalException;

    @Override
    void close() throws RelationalException;
}
