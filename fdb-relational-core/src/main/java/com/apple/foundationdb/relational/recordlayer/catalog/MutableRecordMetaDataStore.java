/*
 * MutableRecordMetaDataStore.java
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

import com.apple.foundationdb.relational.recordlayer.RecordLayerTemplate;

import javax.annotation.Nonnull;
import java.net.URI;

public interface MutableRecordMetaDataStore extends RecordMetaDataStore {
    /**
     * Load the schema template given a template identifier
     *
     * @param templateId the unique ID of the template to load.
     * @return the template information for the specified uniqued identifier.
     */
    RecordLayerTemplate loadTemplate(@Nonnull String templateId);

    /**
     * Add a RecordLayerTemplate to this store
     * There could be multiple templates maintained by this store
     * The templateId got from {@link RecordLayerTemplate#getUniqueId()} is used as the key to uniquely identify the template from the mapping
     *
     * @param schemaTemplate the template to add.
     */
    void addSchemaTemplate(@Nonnull RecordLayerTemplate schemaTemplate);

    /**
     * Assign a schema url to a template irrevocably
     * So the schema specified by the schemaUrl is supposed to use the schema template identified by the templateId
     * One schema can be assigned to a template only once during creation, then it cannot switch to any other template
     *
     * @param schemaUrl the URI for the schema to assign.
     * @param templateId the unique id for the schema template to assign for this schema.
     */
    void assignSchemaToTemplate(@Nonnull URI schemaUrl, @Nonnull String templateId);

}
