/*
 * MatchableTag.java
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

package com.apple.foundationdb.relational.yamltests.tags;

import com.apple.foundationdb.relational.yamltests.Matchers;

import org.jspecify.annotations.Nullable;
import java.util.Optional;

public interface Matchable {

    Matchers.ResultSetMatchResult matches(@Nullable Object other, int rowNumber, String cellRef);

    static Matchers.ResultSetMatchResult prettyPrintError(final String cause, int rowNumber, final String cellRef) {
        return Matchers.ResultSetMatchResult.fail("wrong result at row: " + rowNumber + ", cell: " + cellRef + ", cause: " + cause);
    }

    static Optional<Matchers.ResultSetMatchResult> shouldNotBeNull(@Nullable final Object object, int rowNumber, final String cellRef) {
        if (object != null) {
            return Optional.empty();
        }
        return Optional.of(Matchers.ResultSetMatchResult.fail("unexpected NULL at row: " + rowNumber + ", cell: " + cellRef));
    }
}
