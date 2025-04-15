/*
 * FormatVersionTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FormatVersionTest {

    /**
     * The format versions were just constants, this test was written before the introduction to ensure that the numbers
     * didn't change.
     */
    @Test
    void testOrderingAlignment() {
        final List<Integer> versions = List.of(FDBRecordStore.INFO_ADDED_FORMAT_VERSION,
                FDBRecordStore.RECORD_COUNT_ADDED_FORMAT_VERSION,
                FDBRecordStore.RECORD_COUNT_KEY_ADDED_FORMAT_VERSION,
                FDBRecordStore.FORMAT_CONTROL_FORMAT_VERSION,
                FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION,
                FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION,
                FDBRecordStore.CACHEABLE_STATE_FORMAT_VERSION,
                FDBRecordStore.HEADER_USER_FIELDS_FORMAT_VERSION,
                FDBRecordStore.READABLE_UNIQUE_PENDING_FORMAT_VERSION,
                FDBRecordStore.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE_FORMAT_VERSION);

        assertEquals(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()),
                versions);
        assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION, versions.get(versions.size() - 1));
    }

    @Test
    void testCompleteness() {
        assertEquals(
                IntStream.rangeClosed(FormatVersion.getMinimumVersion().getValueForSerialization(),
                                FormatVersion.getMaximumSupportedVersion().getValueForSerialization())
                        .boxed().collect(Collectors.toList()),
                Arrays.stream(FormatVersion.values())
                        .map(FormatVersion::getValueForSerialization)
                        .sorted()
                        .collect(Collectors.toList()));
    }

    @Test
    void testMinimumVersion() {
        assertEquals(Arrays.stream(FormatVersion.values()).min(Comparator.naturalOrder()).orElseThrow(),
                FormatVersion.getMinimumVersion());
    }

    @Test
    void testMaximumSupportedVersion() {
        assertEquals(Arrays.stream(FormatVersion.values()).max(Comparator.naturalOrder()).orElseThrow(),
                FormatVersion.getMaximumSupportedVersion());
    }

    /**
     * Assert ordering by serialization value aligns with ordering by enum comparison.
     */
    @Test
    void testOrdering() {
        assertEquals(Arrays.stream(FormatVersion.values())
                        .sorted(Comparator.comparing(FormatVersion::getValueForSerialization))
                        .collect(Collectors.toList()),
                Arrays.stream(FormatVersion.values())
                        .sorted()
                        .collect(Collectors.toList()));
    }
}
