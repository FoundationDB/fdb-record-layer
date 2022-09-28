/*
 * SchemaTemplate.java
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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.DescriptorProtos;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * Represents a SchemaTemplate.
 */
public interface SchemaTemplate {

    /**
     * Get the unique identifier for this template.
     *
     * @return the unique identifier for this template. All templates <em>must</em> have a unique name
     */
    String getUniqueId();

    long getVersion();

    RecordMetaDataProto.MetaData getMetaData() throws RelationalException;

    /**
     * Get a new SchemaData instance for this template. This is useful as a starting point
     * for creating new schemas and/or for comparing existing ones.
     * @param databaseId the unique database identifier
     * @param schemaName the name of the schema to generate for
     * @return a new SchemaData instance representing the structure of this template
     * @throws RelationalException if something goes wrong
     */
    Schema generateSchema(String databaseId, String schemaName) throws RelationalException;

    /**
     * Determine if the specified DatabaseSchema instance is valid.
     *
     * @param schema the schema to validate
     * @return true if this schema is valid w.r.t to this template, {@code false} if the schema
     * has "drifted" (i.e. the schema is no longer the same as the template defines). This can happen
     * whenever the template has changed but the schema has not.
     * @throws RelationalException if something went wrong.
     */
    boolean isValid(@Nonnull DatabaseSchema schema) throws RelationalException;

    DescriptorProtos.FileDescriptorProto toProtobufDescriptor();

    @SuppressWarnings("PMD.LooseCoupling")
    LinkedHashSet<TableInfo> getTables();

    Set<TypeInfo> getTypes();
}
