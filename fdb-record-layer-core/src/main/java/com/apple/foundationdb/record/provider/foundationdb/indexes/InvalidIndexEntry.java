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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * An invalid index entry including the entry and the reason why it is invalid.
 */
@API(API.Status.EXPERIMENTAL)
public class InvalidIndexEntry {

    @Nonnull
    private IndexEntry entry;
    @Nonnull
    private Reason reason;

    @Nullable
    private FDBStoredRecord<Message> record;

    private InvalidIndexEntry(@Nonnull IndexEntry entry, @Nonnull Reason reason, @Nullable FDBStoredRecord<Message> record) {
        this.entry = entry;
        this.reason = reason;
        this.record = record;
    }

    /**
     * The reason why an index entry is invalid.
     */
    public interface Reason {
        /**
         * Get the name of the reason.
         *
         * @return the name
         */
        String name();

        /**
         * Get the description of the reason for user displays.
         *
         * @return the user-visible description
         */
        String description();
    }

    /**
     * The reasons supported in the Record Layer.
     */
    public enum Reasons implements Reason {
        ORPHAN("index entry does not point to an existing record"),
        MISSING("index entry is missing for a record"),
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

    public static InvalidIndexEntry newOrphan(@Nonnull IndexEntry entry) {
        return new InvalidIndexEntry(entry, Reasons.ORPHAN, null);
    }

    public static InvalidIndexEntry newMissing(@Nonnull IndexEntry entry, @Nonnull FDBStoredRecord<Message> record) {
        return new InvalidIndexEntry(entry, Reasons.MISSING, record);
    }

    @Nonnull
    public IndexEntry getEntry() {
        return entry;
    }

    @Nonnull
    public Reason getReason() {
        return reason;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InvalidIndexEntry{");
        sb.append("entry=").append(entry);
        sb.append(", reason=").append(reason);
        sb.append(", record=").append(record);
        sb.append('}');
        return sb.toString();
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
        return entry.equals(that.entry) &&
               reason.equals(that.reason) &&
               Objects.equals(record, that.record);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entry, reason, record);
    }
}
