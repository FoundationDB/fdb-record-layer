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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SemanticVersionTest {

    @ParameterizedTest
    @CsvSource({
            "3.5.555.0, 3.5.555.0",
            "4, 4",
            "4.2, 4.2",
            "5.8.9, 5.8.9",
            "9.7.4.3.0, 9.7.4.3.0",
            "5.4.3-SNAPSHOT, 5.4.3-SNAPSHOT",
            "!current_version, !current_version",
            "!min_version, !min_version",
            "!max_version, !max_version",
    })
    void equal(String rawVersionA, String rawVersionB) {
        final SemanticVersion versionA = SemanticVersion.parse(rawVersionA);
        final SemanticVersion versionB = SemanticVersion.parse(rawVersionB);
        Assertions.assertAll(
                () -> assertEquals(0, versionA.compareTo(versionB)),
                () -> assertEquals(0, versionB.compareTo(versionA)));
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
            "3.5.55.0, !current_version",
            "3.5.55.0-SNAPSHOT, !current_version",
            "!current_version, !max_version",
            "!min_version, !current_version",
            "!min_version, !max_version",
            "3.5.55.0, !max_version",
            "3.5.55.0-SNAPSHOT, !max_version",
            "!min_version, 3.5.55.0",
            "!min_version, 3.5.55.0-SNAPSHOT",
            "3.1-SNAPSHOT, !current_version",
            "3.1-SNAPSHOT, !max_version",
            "!min_version, 3.1-SNAPSHOT",
    })
    void notEqual(String rawLowerVersion, String rawHigherVersion) {
        final SemanticVersion lowerVersion = SemanticVersion.parse(rawLowerVersion);
        final SemanticVersion higherVersion = SemanticVersion.parse(rawHigherVersion);
        Assertions.assertAll(
                () -> assertThat(lowerVersion.compareTo(higherVersion)).isLessThan(0),
                () -> assertThat(higherVersion.compareTo(lowerVersion)).isGreaterThan(0));
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
        assertEquals(Integer.signum(Integer.compare(versionA, versionB)),
                Integer.signum(SemanticVersion.parse(versionA + ".0.0").compareTo(
                        SemanticVersion.parse(versionB + ".0.0"))));
    }

    @ParameterizedTest
    @CsvSource({
            "3, 3.0",
            "3, 3.0.0",
            "3, 3.0-SNAPSHOT",
            "3.3, 3.3.0",
            "3.2-SNAPSHOT, 3.2.0",
            "3.2-SNAPSHOT, 3.2.1"
    })
    void incomparable(String rawVersionA, String rawVersionB) {
        final SemanticVersion versionA = SemanticVersion.parse(rawVersionA);
        final SemanticVersion versionB = SemanticVersion.parse(rawVersionB);
        Assertions.assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> versionA.compareTo(versionB)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> versionB.compareTo(versionA)));
    }

    @Test
    void compareToNull() throws Exception {
        final SemanticVersion versionA = SemanticVersion.parse("4.0.559.0");
        assertThrows(NullPointerException.class,
                () -> versionA.compareTo(null));
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
            "4.2.3+boo"
    })
    void badVersion(String rawVersion) throws Exception {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(rawVersion));
    }

    @Test
    void parseNull() throws Exception {
        assertThrows(NullPointerException.class, () -> SemanticVersion.parse(null));
    }

    @Test
    void lesserVersions() {
        final String versionString = "3.5.4.2";
        final List<String> lesserVersions = List.of("2.6.5.3", "3.4.5.3", "3.5.3.3", "3.5.4.1", "3.5.4.2-SNAPSHOT");
        final List<String> greaterVersions = List.of("4.4.3.1", "3.6.3.1", "3.5.5.1", "3.5.4.3", "3.5.4.3-SNAPSHOT");
        final List<SemanticVersion> actualLesserVersions = SemanticVersion.parse(versionString).lesserVersions(
                Stream.concat(lesserVersions.stream(), Stream.concat(Stream.of(versionString), greaterVersions.stream()))
                        .map(SemanticVersion::parse)
                        .collect(Collectors.toSet()));
        actualLesserVersions.sort(Comparator.naturalOrder());
        final List<SemanticVersion> expectedLesserVersions = lesserVersions.stream()
                .map(SemanticVersion::parse).sorted().collect(Collectors.toList());
        assertEquals(expectedLesserVersions, actualLesserVersions);
    }

    @Nonnull
    private static String randomVersionString(Random r) {
        if (r.nextBoolean()) {
            List<SemanticVersion.SemanticVersionType> singletons = Arrays.stream(SemanticVersion.SemanticVersionType.values())
                    .filter(SemanticVersion.SemanticVersionType::isSingleton)
                    .collect(Collectors.toList());
            int choice = r.nextInt(singletons.size());
            return singletons.get(choice).getText();
        } else {
            String version = IntStream.generate(() -> r.nextInt(1000))
                    .limit(4)
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining("."));
            if (r.nextBoolean()) {
                version = version + "-SNAPSHOT";
            }
            return version;
        }
    }

    private static List<String> randomVersionStrings(Random r, int length) {
        return Stream.generate(() -> randomVersionString(r))
                .limit(length)
                .collect(Collectors.toList());
    }

    private void assertSorted(List<SemanticVersion> versions) {
        int pos = 0;
        // First, all the min versions
        while (pos < versions.size() && SemanticVersion.min().equals(versions.get(pos))) {
            pos++;
        }
        // Then all the semantic versions, in order
        SemanticVersion last = null;
        while (pos < versions.size() && SemanticVersion.SemanticVersionType.NORMAL.equals(versions.get(pos).getType())) {
            SemanticVersion current = versions.get(pos);
            if (last != null) {
                assertThat(current)
                        .as("current version %s is out of order in %s", current, versions)
                        .isGreaterThanOrEqualTo(last);
            }
            last = current;
            pos++;
        }
        // Then all the current
        while (pos < versions.size() && SemanticVersion.current().equals(versions.get(pos))) {
            pos++;
        }
        // Then all the max
        while (pos < versions.size() && SemanticVersion.max().equals(versions.get(pos))) {
            pos++;
        }
        assertThat(pos)
                .as("versions %s are out of order starting at position %d", versions, pos)
                .isEqualTo(versions.size());
    }

    static Stream<Arguments> sortRandomVersions() {
        final Random r = new Random(0x5ca1ab13L);
        return Stream.generate(() -> randomVersionStrings(r, 10))
                .limit(100)
                .map(Arguments::of);
    }

    @ParameterizedTest(name = "sortRandomVersions[versions={0}]")
    @MethodSource
    void sortRandomVersions(List<String> versionStrings) {
        List<SemanticVersion> versions = new ArrayList<>(versionStrings.stream().map(SemanticVersion::parse).collect(Collectors.toList()));
        versions.sort(Comparator.naturalOrder());
        assertSorted(versions);
    }
}
