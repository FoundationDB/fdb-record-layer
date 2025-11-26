/*
 * UnableToPlanException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Error being thrown when Cascades cannot plan a query.
 */
@API(API.Status.EXPERIMENTAL)
public class UnableToPlanException extends RecordCoreException {
    private static final long serialVersionUID = -640771754012134420L;

    @Nullable
    private String matchCandidatesInfo;

    public UnableToPlanException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }

    /**
     * Set match candidates information to be included when logging this exception.
     * @param matchCandidatesInfo String representation of match candidates
     * @return this exception for chaining
     */
    @Nonnull
    public UnableToPlanException withMatchCandidatesInfo(@Nullable String matchCandidatesInfo) {
        this.matchCandidatesInfo = matchCandidatesInfo;
        return this;
    }

    /**
     * Get the match candidates information associated with this exception.
     * @return match candidates info string, or null if not set
     */
    @Nullable
    public String getMatchCandidatesInfo() {
        return matchCandidatesInfo;
    }
}
