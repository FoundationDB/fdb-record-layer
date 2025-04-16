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

import com.apple.foundationdb.subspace.Subspace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    public static IntStream testValidateValidFormatVersion() {
        return IntStream.rangeClosed(FormatVersion.getMinimumVersion().getValueForSerialization(),
                FormatVersion.getMaximumSupportedVersion().getValueForSerialization());
    }

    @ParameterizedTest
    @MethodSource
    void testValidateValidFormatVersion(int candidate) {
        assertDoesNotThrow(() -> FormatVersion.validateFormatVersion(candidate, new SubspaceProviderBySubspace(new Subspace())));
    }

    public static IntStream testValidateInvalidFormatVersion() {
        final Set<Integer> valid = testValidateValidFormatVersion().boxed().collect(Collectors.toSet());
        return IntStream.range(FormatVersion.getMinimumVersion().getValueForSerialization() - 10,
                        FormatVersion.getMaximumSupportedVersion().getValueForSerialization() + 10)
                        .filter(candidate -> !valid.contains(candidate));
    }

    @ParameterizedTest
    @MethodSource
    void testValidateInvalidFormatVersion(int candidate) {
        assertThrows(UnsupportedFormatVersionException.class,
                () -> FormatVersion.validateFormatVersion(candidate, new SubspaceProviderBySubspace(new Subspace())));
    }


    public static IntStream testGetValidFormatVersion() {
        return IntStream.rangeClosed(FormatVersion.getMinimumVersion().getValueForSerialization(),
                FormatVersion.getMaximumSupportedVersion().getValueForSerialization());
    }

    @ParameterizedTest
    @EnumSource(FormatVersion.class)
    void testGetValidFormatVersion(FormatVersion candidate) {
        assertEquals(candidate, FormatVersion.getFormatVersion(candidate.getValueForSerialization()));
    }

    public static IntStream testGetInvalidFormatVersion() {
        final Set<Integer> valid = testValidateValidFormatVersion().boxed().collect(Collectors.toSet());
        return IntStream.range(FormatVersion.getMinimumVersion().getValueForSerialization() - 10,
                        FormatVersion.getMaximumSupportedVersion().getValueForSerialization() + 10)
                .filter(candidate -> !valid.contains(candidate));
    }

    @ParameterizedTest
    @MethodSource
    void testGetInvalidFormatVersion(int candidate) {
        assertThrows(UnsupportedFormatVersionException.class,
                () -> FormatVersion.getFormatVersion(candidate));
    }
}
