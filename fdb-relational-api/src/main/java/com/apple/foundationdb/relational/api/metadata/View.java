/*
 * View.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import javax.annotation.Nonnull;

/**
 * Metadata for a non-materialized view, which represents a virtual table defined by a SQL query.
 * <p>
 * The view definition is persisted to disk as a plain SQL string. At runtime, implementations
 * of this interface may cache the compiled logical plan to avoid recompiling the view definition
 * on every access.
 * <p>
 * Views can be either permanent (schema-bound) or temporary (transaction-bound). While the interface
 * supports temporary views, this functionality is not currently implemented.
 */
public interface View extends Metadata {

    /**
     * Returns the SQL definition of this view.
     *
     * @return The SQL query string that defines this view.
     */
    @Nonnull
    String getDescription();

    /**
     * Returns whether this view is temporary (transaction-bound).
     * <p>
     * Note: Temporary views are not currently supported and this method will return {@code false}.
     *
     * @return {@code true} if the view is temporary, {@code false} otherwise.
     */
    boolean isTemporary();

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);
    }
}
