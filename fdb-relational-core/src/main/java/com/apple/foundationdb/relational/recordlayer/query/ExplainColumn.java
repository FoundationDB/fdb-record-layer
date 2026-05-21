/*
 * ExplainColumn.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Columns that can be requested in an EXPLAIN result set.
 * <p>
 * When no column list is specified, {@link #ALL} is used and every column is returned.
 * When a parenthesized list is given — {@code EXPLAIN (PLAN, PLAN_HASH) SELECT ...} — only the
 * requested columns are computed and returned, which avoids generating expensive outputs such as
 * {@link #PLAN_DOT} or {@link #PLAN_GML} when the caller does not need them.
 */
public enum ExplainColumn {
    PLAN,
    PLAN_HASH,
    PLAN_DOT,
    PLAN_GML,
    PLAN_CONTINUATION,
    PLANNER_METRICS;

    public static final Set<ExplainColumn> ALL = EnumSet.allOf(ExplainColumn.class);

    /**
     * Resolves a column name (case-insensitive) to its {@link ExplainColumn}, or throws if unknown.
     */
    @Nonnull
    public static ExplainColumn fromName(@Nonnull String name) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new RelationalException(
                    "Unknown EXPLAIN column: '" + name + "'. Valid columns: PLAN, PLAN_HASH, PLAN_DOT, PLAN_GML, PLAN_CONTINUATION, PLANNER_METRICS",
                    ErrorCode.INVALID_PARAMETER, e).toUncheckedWrappedException();
        }
    }
}
