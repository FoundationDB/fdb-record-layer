/*
 * CodeVersionTest.java
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

package com.apple.foundationdb.relational.yamltests.server;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class CodeVersionTest {
    private static String randomVersionString(Random r) {
        if (r.nextBoolean()) {
            SpecialCodeVersion.SpecialCodeVersionType[] types = SpecialCodeVersion.SpecialCodeVersionType.values();
            int choice = r.nextInt(types.length);
            return types[choice].getText();
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

    private void assertSorted(List<CodeVersion> versions) {
        int pos = 0;
        // First, all the min versions
        while (pos < versions.size() && versions.get(pos).equals(SpecialCodeVersion.min())) {
            pos++;
        }
        // Then all the semantic versions, in order
        SemanticVersion last = null;
        while (pos < versions.size() && versions.get(pos) instanceof SemanticVersion) {
            SemanticVersion current = (SemanticVersion) versions.get(pos);
            if (last != null) {
                assertThat(current)
                        .as("current version %s is out of order in %s", current, versions)
                        .isGreaterThanOrEqualTo(last);
            }
            last = current;
            pos++;
        }
        // Then all the current
        while (pos < versions.size() && versions.get(pos).equals(SpecialCodeVersion.current())) {
            pos++;
        }
        // Then all the max
        while (pos < versions.size() && versions.get(pos).equals(SpecialCodeVersion.max())) {
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
        List<CodeVersion> versions = new ArrayList<>(versionStrings.stream().map(CodeVersion::parse).collect(Collectors.toList()));
        versions.sort(Comparator.naturalOrder());
        assertSorted(versions);
    }
}
