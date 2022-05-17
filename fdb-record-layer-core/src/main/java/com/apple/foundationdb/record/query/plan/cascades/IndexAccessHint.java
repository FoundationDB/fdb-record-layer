/*
 * IndexAccessHint.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents an index hint.
 */
public class IndexAccessHint implements AccessHint {
    @Nonnull
    private final String indexName;

    public IndexAccessHint(@Nonnull final String indexName) {
        this.indexName = indexName;
    }

    @Override
    @Nonnull
    public String getAccessHintType() {
        return "INDEX";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return indexName.equals(((IndexAccessHint)other).getIndexName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName);
    }

    @Nonnull
    public String getIndexName() {
        return indexName;
    }

}
