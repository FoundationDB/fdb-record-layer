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

package com.apple.foundationdb.relational.api.catalog;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Represents a SchemaTemplate
 */
public interface SchemaTemplate {

    /**
     * @return the unique identifier for this template. All templates <em>must</em> have a unique name
     */
    URI getUniqueName();

    /**
     * @param schema the schema to validate
     * @return true if this schema is valid w.r.t to this template, {@code false} if the schema
     * has "drifted" (i.e. the schema is no longer the same as the template defines). This can happen
     * whenever the template has changed but the schema has not.
     */
    boolean isValid(@Nonnull DatabaseSchema schema);
}
