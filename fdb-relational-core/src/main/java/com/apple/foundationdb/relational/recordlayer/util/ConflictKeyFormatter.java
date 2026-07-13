/*
 * ConflictKeyFormatter.java
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for rendering FDB conflict ranges (returned by
 * {@code FDBRecordContext.getNotCommittedConflictingKeys()}) into strings suitable for logging
 * and inclusion in exception messages.
 *
 * <p>For each key we attempt to parse it as an FDB {@link Tuple}; if that fails we fall back to
 * {@link ByteArrayUtil#printable(byte[])} which produces the same escaping FDB tooling uses (e.g.
 * {@code \xff/metadataVersion}).</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class ConflictKeyFormatter {

    private ConflictKeyFormatter() {
    }

    /**
     * Render a list of conflict ranges as a compact human-readable string.
     * Returns {@code "[]"} for an empty list, {@code "null"} for null.
     *
     * @param ranges the conflict ranges, or null
     * @return a comma-separated description of the ranges enclosed in brackets
     */
    @Nonnull
    public static String formatRanges(@Nullable List<Range> ranges) {
        if (ranges == null) {
            return "null";
        }
        if (ranges.isEmpty()) {
            return "[]";
        }
        return ranges.stream()
                .map(ConflictKeyFormatter::formatRange)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    /**
     * Render a single conflict range as {@code "begin..end"} using {@link #formatKey(byte[])} for each bound.
     * @param range the range
     * @return a printable description
     */
    @Nonnull
    public static String formatRange(@Nonnull Range range) {
        return formatKey(range.begin) + ".." + formatKey(range.end);
    }

    /**
     * Render a single FDB key. First attempts to decode as a {@link Tuple}; if that fails, falls back to
     * {@link ByteArrayUtil#printable(byte[])}.
     * @param key the raw key bytes, may be null
     * @return a printable description
     */
    @Nonnull
    public static String formatKey(@Nullable byte[] key) {
        if (key == null) {
            return "null";
        }
        if (key.length == 0) {
            return "\"\"";
        }
        // System-key-space keys start with 0xff; don't try to Tuple-decode them (they aren't tuples).
        if ((key[0] & 0xff) != 0xff) {
            try {
                final Tuple t = Tuple.fromBytes(key);
                return t.toString();
            } catch (RuntimeException ignored) {
                // Fall through to raw byte rendering.
            }
        }
        return ByteArrayUtil.printable(key);
    }
}
