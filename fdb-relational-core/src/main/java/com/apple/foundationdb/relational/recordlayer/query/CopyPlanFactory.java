/*
 * CopyPlanFactory.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Factory for creating COPY command query plans.
 */
@API(API.Status.EXPERIMENTAL)
public final class CopyPlanFactory {

    private CopyPlanFactory() {
        // Utility class
    }

    /**
     * Creates a COPY export plan.
     *
     * @param path the KeySpace path to export from (e.g., "/FRL/MY_DATABASE")
     *
     * @return a CopyPlan for exporting data
     */
    @Nonnull
    public static CopyPlan getCopyExportAction(@Nonnull String path,
                                               @Nonnull QueryExecutionContext queryExecutionContext) {
        // Export returns a single BYTES column (not nullable)
        Type rowType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(
                        Type.primitiveType(Type.TypeCode.BYTES, false),
                        java.util.Optional.of("DATA"),
                        java.util.Optional.of(0))));
        return new CopyPlan(CopyPlan.CopyType.EXPORT, path, rowType, queryExecutionContext);
    }

    /**
     * Creates a COPY import plan.
     *
     * @param path the KeySpace path to import into (e.g., "/FRL/MY_DATABASE")
     *
     * @return a CopyPlan for importing data
     */
    @Nonnull
    public static CopyPlan getCopyImportAction(@Nonnull String path,
                                               @Nonnull QueryExecutionContext queryExecutionContext) {
        // Import returns a single INT column (not nullable) for count
        Type rowType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(
                        Type.primitiveType(Type.TypeCode.INT, false),
                        java.util.Optional.of("COUNT"),
                        java.util.Optional.of(0))));

        return new CopyPlan(CopyPlan.CopyType.IMPORT, path, rowType, queryExecutionContext);
    }
}
