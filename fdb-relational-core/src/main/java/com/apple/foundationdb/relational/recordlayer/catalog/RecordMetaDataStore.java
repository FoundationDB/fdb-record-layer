/*
 * RecordMetaDataStore.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;

public interface RecordMetaDataStore {
    /**
     * Load the metadata provider given a schema url.
     *
     * @param schemaUrl the URI for the schema to get metadata for.
     * @return the metadata for the schema.
     * @throws RelationalException if the metadata cannot be loaded.
     */
    RecordMetaDataProvider loadMetaData(@Nonnull URI schemaUrl) throws RelationalException;
}
