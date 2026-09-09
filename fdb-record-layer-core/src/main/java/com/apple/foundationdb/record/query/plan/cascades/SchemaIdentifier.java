/*
 * SchemaIdentifier.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Identifies a schema (record store) within a query plan. A {@code null} schema name means "the
 * schema of the current connection" — the default for single-schema queries.
 *
 * <p>Used to tag scan expressions ({@link com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression},
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression})
 * so that {@code ImplementNestedLoopJoinRule} can detect when both sides of a join belong to
 * different schemas and wrap the inner side in a {@code RecordQueryStoreBindingPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public final class SchemaIdentifier {

    private static final SchemaIdentifier CURRENT = new SchemaIdentifier(null);

    @Nullable
    private final String schemaName;

    private SchemaIdentifier(@Nullable final String schemaName) {
        this.schemaName = schemaName;
    }

    /** Returns the singleton representing the current connection's schema. */
    @Nonnull
    public static SchemaIdentifier current() {
        return CURRENT;
    }

    /** Returns a {@code SchemaIdentifier} for the named schema. */
    @Nonnull
    public static SchemaIdentifier of(@Nonnull final String schemaName) {
        return new SchemaIdentifier(schemaName);
    }

    /** Returns {@code true} if this identifier represents the current connection schema. */
    public boolean isCurrentSchema() {
        return schemaName == null;
    }

    /** Returns the schema name, or {@code null} if this is the current connection schema. */
    @Nullable
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return Objects.equals(schemaName, ((SchemaIdentifier) other).schemaName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schemaName);
    }

    @Override
    public String toString() {
        return schemaName == null ? "<current>" : schemaName;
    }
}
