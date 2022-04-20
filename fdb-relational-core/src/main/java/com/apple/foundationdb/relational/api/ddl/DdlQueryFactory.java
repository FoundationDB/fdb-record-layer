/*
 * DdlQueryFactory.java
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

package com.apple.foundationdb.relational.api.ddl;

import java.net.URI;

import javax.annotation.Nonnull;

/**
 * Placeholder factory for creating DdlQuery entities.
 *
 * Eventually, this should be supplanted by a general-purpose query engine which can read the Catalog as if
 * it were any other database, but for now.
 */
public interface DdlQueryFactory {

    DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath);

    DdlQuery getListSchemasQueryAction(@Nonnull URI dbPath);

    DdlQuery getListSchemaTemplatesQueryAction();

    DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId);

    DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbId, @Nonnull String schemaId);
}
