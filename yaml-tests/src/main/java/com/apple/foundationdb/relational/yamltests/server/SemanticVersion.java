/*
 * SemanticVersion.java
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


import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Object representing a version of the Record Layer.
 * <p>
 *     This is inspired by the following three version specifications:
 *     <ul>
 *         <li><a href="https://semver.org">semver.org</a>
 *         <li><a href="https://docs.gradle.org/current/userguide/dependency_versions.html#sec:version-ordering">gradle's version ordering</a>
 *         <li><a href="https://maven.apache.org/ref/3.3.3/maven-artifact/apidocs/org/apache/maven/artifact/versioning/ComparableVersion.html">maven's version class</a>,
 *         which is also outlined on <a href="https://cwiki.apache.org/confluence/display/MAVENOLD/Versioning">their wiki</a>.
 *     </ul>
 *     Gradle and Maven are the most important, as this is a java project, but they have complicated semantics, and I
 *     had trouble reasoning about how they interact with non-ascii characters, and they appear to disagree.
 *     Thus, this spec implements only slightly more than what the Record Layer needs, which is to say:
 *     a sequence of positive numbers ({@code 0|[1-9]\\d*} separated by {@code .}s, and an optional {@code -SNAPSHOT}
 *     at the end. Each version component is compared as integers, and versions with a different number of components
 *     are not comparable. A version with {@code -SNAPSHOT} version added to the less than the same set of numbers
 *     without {@code -SNAPSHOT}.
 * @see <a href="https://github.com/FoundationDB/fdb-record-layer/blob/main/docs/Versioning.md#semantic-versioning">Versioning</a>
 */
public class SemanticVersion implements Comparable<SemanticVersion> {
    private static final String NUMBER_PATTERN = "(?:0|[1-9]\\d*)";
    private static final String ALPHANUMERIC_PATTERN = "\\d*[a-zA-Z-][0-9a-zA-Z-]";
    private static final String PRE_RELEASE_PART = "(?:" + NUMBER_PATTERN + "|" + ALPHANUMERIC_PATTERN + ")";
    private static final String BUILD_PART = "(?:\\d+|" + ALPHANUMERIC_PATTERN + ")";
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "^(?<versionNumbers>" + NUMBER_PATTERN + "(?:\\." + NUMBER_PATTERN + ")*)" +
                    "(?:-(?<prerelease>" + dotSeparated(PRE_RELEASE_PART) + "))?" +
                    "(?:\\+(?<buildMetadata>" + dotSeparated(BUILD_PART) + "))?$");
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("\\d+");

    private final List<Integer> versionNumbers;
    private final List<String> prerelease;
    private final String buildMetadata;

    public SemanticVersion(List<Integer> versionNumbers, List<String> prerelease, String buildMetadata) {
        this.versionNumbers = versionNumbers;
        this.prerelease = prerelease;
        this.buildMetadata = buildMetadata;
    }

    public static SemanticVersion parse(String versionString) {
        final Matcher matcher = VERSION_PATTERN.matcher(versionString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Version is not valid: " + versionString);
        }
        List<Integer> versionNumbers = Arrays.stream(matcher.group("versionNumbers").split("\\."))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        final String prereleaseString = matcher.group("prerelease");
        if (prereleaseString != null && !prereleaseString.equals("SNAPSHOT")) {
            throw new IllegalArgumentException("Only SNAPSHOT (all caps) pre-release is supported right now");
        }
        final List<String> prerelease = prereleaseString == null ? List.of() : List.of(prereleaseString.split("\\."));
        final String buildMetadata = matcher.group("buildMetadata") == null ? "" : matcher.group("buildMetadata");
        if (!buildMetadata.isEmpty()) {
            throw new IllegalArgumentException("Build MetaData is not currently supported");
        }
        return new SemanticVersion(versionNumbers, prerelease, buildMetadata);
    }

    private static String dotSeparated(String part) {
        return part + "(." + part + ")*";
    }

    @Override
    public String toString() {
        String version = versionNumbers.stream().map(Object::toString).collect(Collectors.joining("."));
        if (!prerelease.isEmpty()) {
            version = version + "-" + prerelease;
        }
        if (!buildMetadata.isEmpty()) {
            version = version + "+" + buildMetadata;
        }
        return version;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SemanticVersion that = (SemanticVersion)o;
        return Objects.equals(versionNumbers, that.versionNumbers) && Objects.equals(prerelease, that.prerelease) && Objects.equals(buildMetadata, that.buildMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionNumbers, prerelease, buildMetadata);
    }

    @Override
    public int compareTo(SemanticVersion o) {
        // negative if this is less than o
        final int versionComparison = compareVersionNumbers(o);
        if (versionComparison != 0) {
            return versionComparison;
        }
        return comparePrerelease(o);
    }

    private int compareVersionNumbers(SemanticVersion o) {
        // semver only allows 3, we only use 4.
        // gradle supports an arbitrary number, but it gets complicated if there is a pre-release or build info
        if (versionNumbers.size() != o.versionNumbers.size()) {
            throw new IllegalArgumentException("Versions only differ by part count" + this + " vs " + o);
        }
        int length = versionNumbers.size();
        for (int i = 0; i < length; i++) {
            if (versionNumbers.get(i) < o.versionNumbers.get(i)) {
                return -1;
            } else if (versionNumbers.get(i) > o.versionNumbers.get(i)) {
                return 1;
            }
        }
        return 0;
    }

    private int comparePrerelease(SemanticVersion o) {
        if (!prerelease.isEmpty() && o.prerelease.isEmpty()) {
            return -1;
        } else if (prerelease.isEmpty() && !o.prerelease.isEmpty()) {
            return 1;
        }
        int length = Math.min(prerelease.size(), o.prerelease.size());
        for (int i = 0; i < length; i++) {
            String thisPart = prerelease.get(i);
            String otherPart = o.prerelease.get(i);
            if (NUMERIC_PATTERN.matcher(thisPart).matches()) {
                if (NUMERIC_PATTERN.matcher(otherPart).matches()) {
                    int thisNumber = Integer.parseInt(thisPart);
                    int otherNumber = Integer.parseInt(otherPart);
                    if (thisNumber < otherNumber) {
                        return -1;
                    } else if (thisNumber > otherNumber) {
                        return 1;
                    }
                } else {
                    return -1;
                }
            } else {
                if (NUMERIC_PATTERN.matcher(otherPart).matches()) {
                    return 1;
                } else {
                    int comparison = thisPart.compareTo(otherPart);
                    if (comparison < 0) {
                        return -1;
                    } else if (comparison > 0) {
                        return 1;
                    }
                }
            }
        }
        if (prerelease.size() < o.prerelease.size()) {
            return -1;
        } else if (prerelease.size() > o.prerelease.size()) {
            return 1;
        }
        return 0;
    }
}
