/*
 * SchemaExistsBehavior.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

/**
 * Governs what {@link StoreCatalog#saveSchema} does when a schema already exists at the
 * target {@code (databaseId, schemaName)} coordinate.
 *
 * <p>The four variants map to the "if-exists" decisions that callers of {@code saveSchema}
 * used to encode ad-hoc at the call site (pre-load + throw, gate on {@code doesSchemaExist},
 * overwrite blindly, etc.). Pushing them into the enum forces every caller to declare its
 * intent and lets the catalog enforce that intent uniformly.</p>
 */
public enum SchemaExistsBehavior {
    /**
     * Throw {@code SCHEMA_ALREADY_EXISTS} if a schema is already present at
     * {@code (databaseId, schemaName)}.
     */
    ERROR,

    /**
     * If a schema is already present at {@code (databaseId, schemaName)}, compare the existing
     * row to the schema being saved:
     * <ul>
     *   <li>identical → no-op, return without writing; this implies that this transaction won't cause other 
     *       transactions to conflict because they read the schema.</li>
     *   <li>different → throw {@code SCHEMA_ALREADY_EXISTS}.</li>
     * </ul>
     */
    ERROR_IF_DIFFERENT,

    /**
     * Silently succeed if a schema is already present at {@code (databaseId, schemaName)},
     * regardless of whether the existing schema matches the one being saved. No comparison is
     * performed and no write is issued in the exists branch. Useful for idempotent init paths
     * that must survive concurrent invocation.
     */
    DO_NOTHING,

    /**
     * Only allow the save when the existing schema is a valid upgrade target for the schema
     * being saved. Concretely, the existing schema must have the same template name; then:
     * <ul>
     *   <li>new template version {@code >} existing → write the new row (the existing row is
     *       overwritten);</li>
     *   <li>new template version {@code ==} existing → no-op, return without writing (keeps
     *       the transaction read-only w.r.t. this schema row, so concurrent same-version
     *       upgrades do not collide);</li>
     *   <li>new template version {@code <} existing, OR template name differs → throw
     *       {@code SCHEMA_ALREADY_EXISTS}.</li>
     * </ul>
     */
    UPGRADE
}
