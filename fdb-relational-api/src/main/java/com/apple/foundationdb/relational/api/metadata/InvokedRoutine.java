/*
 * SqlFunction.java
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
 * Base interface for all invoked routines, such as SQL functions.
 */
public interface InvokedRoutine extends Metadata {

    enum Language {
        SQL,
        JAVA
    }

    /**
     * Returns the description of the routine, in case of SQL functions, this would be the actual
     * SQL definition of the function.
     *
     * @return The description of the routine.
     */
    @Nonnull
    String getDescription();

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);
    }
}
