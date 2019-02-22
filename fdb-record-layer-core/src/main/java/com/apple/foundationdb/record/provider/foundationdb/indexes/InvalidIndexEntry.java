/*
 * InvalidIndexEntry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.IndexEntry;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * An invalid index entry including the entry and the reason why it is invalid.
 */
public class InvalidIndexEntry {

    @Nonnull
    private IndexEntry entry;
    @Nonnull
    private Reason reason;

    /**
     * The reason why an index entry is invalid.
     */
    public interface Reason {
        /**
         * Get the name of the reason.
         * @return the name
         */
        String name();

        /**
         * Get the description of the reason for user displays.
         * @return the user-visible description
         */
        String description();
    }

    /**
     * The reasons supported in the Record Layer.
     */
    public enum Reasons implements Reason {
        ORPHAN("index entry does not point to an existing record")
        ;

        private final String description;

        Reasons(String description) {
            this.description = description;
        }

        @Override
        public String description() {
            return description;
        }
    }

    @Nonnull
    public IndexEntry getEntry() {
        return entry;
    }

    @Nonnull
    public Reason getReason() {
        return reason;
    }

    public InvalidIndexEntry(@Nonnull IndexEntry entry, @Nonnull Reason reason) {
        this.entry = entry;
        this.reason = reason;
    }

    @Override
    public String toString() {
        return reason + " - " + entry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InvalidIndexEntry that = (InvalidIndexEntry)o;
        return Objects.equals(entry, that.entry) &&
               Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entry, reason);
    }
}
