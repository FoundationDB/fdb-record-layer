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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;

import javax.annotation.Nonnull;

/**
 * Governs what {@link StoreCatalog#saveSchema} does when a schema already exists at the
 * target {@code (databaseId, schemaName)} coordinate.
 *
 * <p>The four variants map to the "if-exists" decisions that callers of {@code saveSchema}
 * used to encode ad-hoc at the call site (pre-load + throw, gate on {@code doesSchemaExist},
 * overwrite blindly, etc.). Pushing them into the enum forces every caller to declare its
 * intent and lets the catalog enforce that intent uniformly.</p>
 *
 * <p>The compare-and-decide logic itself lives on the enum via
 * {@link #shouldWrite(Schema, Schema)}, so every {@code StoreCatalog} implementation can share
 * one decision path rather than each open-coding an identical switch.</p>
 */
public enum SchemaExistsBehavior {
    /**
     * Throw {@code SCHEMA_ALREADY_EXISTS} if a schema is already present at {@code (databaseId, schemaName)}.
     */
    ERROR {
        @Override
        public boolean shouldWrite(@Nonnull Schema newSchema, @Nonnull Schema existingSchema) throws RelationalException {
            throw new RelationalException("Schema " + newSchema.getDatabaseName() + "/" + newSchema.getName() +
                    " already exists.", ErrorCode.SCHEMA_ALREADY_EXISTS);
        }
    },

    /**
     * A no-op if the requested schema is the same as the one that exists, otherwise fail.
     */
    ERROR_IF_DIFFERENT {
        @Override
        public boolean shouldWrite(@Nonnull Schema newSchema, @Nonnull Schema existingSchema) throws RelationalException {
            if (areSchemasIdentical(newSchema, existingSchema)) {
                return false;
            }
            throw new RelationalException("Schema " + newSchema.getDatabaseName() + "/" + newSchema.getName() +
                    " already exists with a different template (" +
                    existingSchema.getSchemaTemplate().getName() + "@" + existingSchema.getSchemaTemplate().getVersion() +
                    " vs " + newSchema.getSchemaTemplate().getName() + "@" + newSchema.getSchemaTemplate().getVersion() + ").",
                    ErrorCode.SCHEMA_ALREADY_EXISTS);
        }
    },

    /**
     * Silently succeed if a schema is already present at {@code (databaseId, schemaName)},
     * regardless of whether the existing schema matches the one being saved. No comparison is
     * performed and no write is issued in the exists branch. This aligns with common {@code IF EXISTS} behavior.
     */
    DO_NOTHING {
        @Override
        public boolean shouldWrite(@Nonnull Schema newSchema, @Nonnull Schema existingSchema) {
            return false;
        }
    },

    /**
     * Only allow the save when the existing schema is a valid upgrade target for the schema being saved.
     */
    UPGRADE {
        @Override
        public boolean shouldWrite(@Nonnull Schema newSchema, @Nonnull Schema existingSchema) throws RelationalException {
            final String existingTemplateName = existingSchema.getSchemaTemplate().getName();
            final String newTemplateName = newSchema.getSchemaTemplate().getName();
            if (!existingTemplateName.equals(newTemplateName)) {
                throw new RelationalException("Cannot upgrade schema " +
                        newSchema.getDatabaseName() + "/" + newSchema.getName() +
                        ": existing template " + existingTemplateName + " does not match new template " +
                        newTemplateName + ".",
                        ErrorCode.SCHEMA_ALREADY_EXISTS);
            }
            final int existingVersion = existingSchema.getSchemaTemplate().getVersion();
            final int newVersion = newSchema.getSchemaTemplate().getVersion();
            if (newVersion < existingVersion) {
                throw new RelationalException("Cannot upgrade schema " +
                        newSchema.getDatabaseName() + "/" + newSchema.getName() +
                        ": new template version " + newVersion + " is lower than existing version " + existingVersion + ".",
                        ErrorCode.SCHEMA_ALREADY_EXISTS);
            }
            // When strictly greater, write; on equality, no-op.
            return newVersion > existingVersion;
        }
    };

    /**
     * Decide what a {@code saveSchema} caller should do when the schema already exists in the catalog.
     * Returning {@code true} tells the caller to overwrite the on-disk row with {@code newSchema};
     * returning {@code false} tells the caller to leave the on-disk row untouched (a no-op).
     * Throwing {@code RelationalException} with {@link ErrorCode#SCHEMA_ALREADY_EXISTS} tells the caller to refuse
     * the save.
     *
     * @param newSchema      the schema the caller was asked to save
     * @param existingSchema the schema currently persisted at {@code (dbId, schemaName)}
     * @return {@code true} to write {@code newSchema} over the existing row, {@code false} to
     *         return without writing
     * @throws RelationalException with {@link ErrorCode#SCHEMA_ALREADY_EXISTS} when this
     *                             behavior refuses the save
     */
    public abstract boolean shouldWrite(@Nonnull Schema newSchema, @Nonnull Schema existingSchema) throws RelationalException;

    /**
     * Return if the two provided schemas are identical.
     * @return {@code true} if and only if the two schemas are identical
     */
    private static boolean areSchemasIdentical(@Nonnull Schema a, @Nonnull Schema b) {
        return a.getSchemaTemplate().getName().equals(b.getSchemaTemplate().getName())
                && a.getSchemaTemplate().getVersion() == b.getSchemaTemplate().getVersion();
    }

}
