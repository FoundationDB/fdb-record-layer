/*
 * SemanticVersionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SemanticVersionTest {

    @ParameterizedTest
    @CsvSource({
            "3.5.555.0, 3.5.555.0",
            "4, 4",
            "4.2, 4.2",
            "5.8.9, 5.8.9",
            "9.7.4.3.0, 9.7.4.3.0",
            "5.4.3-SNAPSHOT, 5.4.3-SNAPSHOT"
    })
    void equal(String rawVersionA, String rawVersionB) {
        final SemanticVersion versionA = SemanticVersion.parse(rawVersionA);
        final SemanticVersion versionB = SemanticVersion.parse(rawVersionB);
        Assertions.assertAll(
                () -> assertEquals(0, versionA.compareTo(versionB)),
                () -> assertEquals(0, versionA.compareTo(versionB)));
    }

    @ParameterizedTest
    @CsvSource({
            "3, 4",
            "1.1, 1.2",
            "3.4.2.0-SNAPSHOT, 3.4.2.0",
            "3.4.2.1-SNAPSHOT, 3.4.2.1",
            "3.4.2-SNAPSHOT, 3.4.2",
            "3.1, 3.2-SNAPSHOT",
            "3.5.5.0, 3.5.12.0",
            "3.5.55.0, 3.5.55.1",
            "3.5.55.1, 3.5.56.0",
            "3.5.55.0, 3.55.5.0",
    })
    void notEqual(String rawLowerVersion, String rawHigherVersion) {
        final SemanticVersion lowerVersion = SemanticVersion.parse(rawLowerVersion);
        final SemanticVersion higherVersion = SemanticVersion.parse(rawHigherVersion);
        Assertions.assertAll(
                () -> assertEquals(-1, lowerVersion.compareTo(higherVersion)),
                () -> assertEquals(1, higherVersion.compareTo(lowerVersion)));
    }

    @ParameterizedTest
    @CsvSource({
            "3, 4",
            "4, 3",
            "4, 4",
    })
    void alignsWithComparator(int versionA, int versionB) {
        // Ensure that the signage aligns with the standard `compare` methods
        // the value doesn't have to, but it does, and this is easier than asserting that they have the same sign
        assertEquals(Integer.compare(versionA, versionB),
                SemanticVersion.parse(versionA + ".0.0").compareTo(
                        SemanticVersion.parse(versionB + ".0.0")));
    }

    @ParameterizedTest
    @CsvSource({
            "3, 3.0",
            "3, 3.0.0",
            "3, 3.0-SNAPSHOT",
            "3.3, 3.3.0",
            "3.2-SNAPSHOT, 3.2.0",
            "3.2-SNAPSHOT, 3.2.1",
    })
    void incomparible(String rawVersionA, String rawVersionB) {
        final SemanticVersion versionA = SemanticVersion.parse(rawVersionA);
        final SemanticVersion versionB = SemanticVersion.parse(rawVersionB);
        Assertions.assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> versionA.compareTo(versionB)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> versionA.compareTo(versionB)));
    }

    // Comparing pre-releases is complicated, but we only ever use "SNAPSHOT", so forbid everything else.
    @ParameterizedTest
    @ValueSource(strings = {
            "badVersion",
            "00002",
            "3-4-3-2",
            "3.0-RC",
            "3.2.3.4-RC",
            "3.2.3.0-snapshot",
            "3.2.3.0-4",
            "3.2.3.0-SNAPSHOT.4",
            "3.2-RELEASE",
            "1.a",
            "3.4.a",
            "4.2.3+boo",
    })
    void badVersion(String rawVersion) throws Exception {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(rawVersion));
    }
}
