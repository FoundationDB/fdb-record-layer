/*
 * Typed.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.typing;

import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens;

import javax.annotation.Nonnull;

/**
 * Provides {@link Type} information about result set. Implementations of this interface allow the caller to inspect
 * the {@link Type} of their result sets.
 */
public interface Typed {

    /**
     * Returns the {@link Type} of the result set.
     * @return the {@link Type} of the result set.
     */
    @Nonnull
    Type getResultType();

    /**
     * Returns a human-friendly textual representation of both the type-producing instance and its result set {@link Type}.
     * @return a token list used to render a human-friendly textual representation of both the type-producing instance
     *         and its result set {@link Type}.
     */
    @Nonnull
    default ExplainTokens describe() {
        return getResultType().describe();
    }
}
