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


import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
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
 *     are not comparable. A version with {@code -SNAPSHOT} version added to a version the less than the same set of
 *     numbers without {@code -SNAPSHOT} (e.g. {@code 3.4.5.1-SNAPSHOT < 3.4.5.1}).
 *</p>
 *
 * <p>
 * In addition, there are a set of "special" versions included that are not based on the version numbers. They
 * stand in for special versions that are not encompassed by the normal version space. They are:
 * </p>
 * <ul>
 *     <li>
 *         {@link #min()}: The minimum possible version. This is useful for specifying the endpoint of open version
 *         range. For example, set of all versions less than {@code 3.4.5.1} can be expressed as all versions between
 *         {@link #min()} and {@code 3.4.5.1}.
 *     </li>
 *     <li>
 *         {@link #current()}: The current version of the code. Before code is committed and released, it is useful to
 *         have a pointer to what <em>will</em> be the code's version even if it isn't known. For example, one might
 *         want to assert on what the behavior of any version older than the current version was. The {@link #current()}
 *         version provides that context, and it is assumed to be greater than all other versions (except the
 *         {@link #max()} version).
 *     </li>
 *     <li>
 *         {@link #max()}: The maximum possible version. Like {@link #min()}, this is also useful for specifying the
 *         endpoint of an open range.
 *     </li>
 * </ul>
 * @see <a href="https://github.com/FoundationDB/fdb-record-layer/blob/main/docs/Versioning.md#semantic-versioning">Versioning</a>
 */
public class SemanticVersion implements Comparable<SemanticVersion> {
    private static final String NUMBER_PATTERN = "(?:0|[1-9]\\d*)";
    private static final String ALPHANUMERIC_PATTERN = "\\d*[a-zA-Z-][0-9a-zA-Z-]";
    private static final String PRE_RELEASE_PART = "(?:" + NUMBER_PATTERN + "|" + ALPHANUMERIC_PATTERN + ")";
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "^(?<versionNumbers>" + NUMBER_PATTERN + "(?:\\." + NUMBER_PATTERN + ")*)" +
                    "(?:-(?<prerelease>" + dotSeparated(PRE_RELEASE_PART) + "))?");

    /**
     * Enum representing the type of this version. The {@link SemanticVersion} class is
     * modeled like an algebraic data type that might look like, say:
     *
     * <pre>{@code
     * enum SemanticVersion {
     *     case MIN
     *     case NORMAL(versionNumbers: [Integer], preRelease: [String]),
     *     case CURRENT
     *     case MAX
     * }
     * }</pre>
     *
     * <p>
     * The {@code versionNumbers} and {@code preRelease} are only really relevant
     * for the {@link #NORMAL} case. The other three cases are all singletons and do not
     * contain any state themselves.
     * </p>
     *
     * <p>
     * The order here is important. Versions are ordered first by their type, and then
     * by their other data (if appropriate).
     * </p>
     */
    public enum SemanticVersionType {
        MIN("!min_version", true),
        NORMAL("", false),
        CURRENT("!current_version", true),
        MAX("!max_version", true),
        ;

        @Nonnull
        private final String text;
        private final boolean singleton;

        SemanticVersionType(String text, boolean singleton) {
            this.text = text;
            this.singleton = singleton;
        }

        @Nonnull
        public String getText() {
            return text;
        }

        public boolean isSingleton() {
            return singleton;
        }
    }

    @Nonnull
    private static final EnumMap<SemanticVersionType, SemanticVersion> SINGLETONS = new EnumMap<>(SemanticVersionType.class);

    static {
        Arrays.stream(SemanticVersionType.values()).forEach(type -> {
            if (type.isSingleton()) {
                SINGLETONS.put(type, new SemanticVersion(type, Collections.emptyList(), Collections.emptyList()));
            }
        });
    }

    /**
     * The type of the semantic version. Determines whether this is a normal semantic version
     * or one of the special stand-ins. See {@link SemanticVersionType} for more information.
     *
     * @see SemanticVersionType
     */
    @Nonnull
    private final SemanticVersionType type;

    /**
     * The main version components.
     * <p>
     *     For the current record layer versioning, this would be {@code List.of(major, minor, build, patch)}, but this
     *     class supports any number of version components.
     *     When comparing, these are compared in order.
     */
    @Nonnull
    private final List<Integer> versionNumbers;
    /**
     * The prerelease metadata, but only {@code SNAPSHOT} is supported.
     * <p>
     *     We only support {@code List.of("SNAPSHOT")} or {@code List.of()}, because that's the only one we use,
     *     and gradle and maven have complicated comparison that disagrees. A version that has the same value
     *     {@link #versionNumbers} but an empty {@code prerelease} is greater than one with a non-empty {@code prerelease}.
     */
    @Nonnull
    private final List<String> prerelease;

    private SemanticVersion(@Nonnull SemanticVersionType type, @Nonnull List<Integer> versionNumbers, @Nonnull List<String> prerelease) {
        this.type = type;
        this.versionNumbers = versionNumbers;
        this.prerelease = prerelease;
    }

    /**
     * Get the special "point at negative infinity". Useful for range endpoints.
     *
     * @return a special version that is less than all other versions
     */
    @Nonnull
    public static SemanticVersion min() {
        return SINGLETONS.get(SemanticVersionType.MIN);
    }

    /**
     * Get the special version indicating the current code version. Useful for asserting
     * about behavior that differs between the current version and previous releases.
     *
     * @return a special version that represents the current (potentially unreleased) code version
     */
    @Nonnull
    public static SemanticVersion current() {
        return SINGLETONS.get(SemanticVersionType.CURRENT);
    }

    /**
     * Get the special "point at positive infinity". Useful for range endpoints.
     *
     * @return a special version that is greater than all other versions
     */
    @Nonnull
    public static SemanticVersion max() {
        return SINGLETONS.get(SemanticVersionType.MAX);
    }

    /**
     * Parse a version string (e.g. {@code 4.0.559.0} or {@code 4.0.560.0-SNAPSHOT}.
     * @param versionString a version in string form
     * @return a parsed version
     */
    @Nonnull
    public static SemanticVersion parse(@Nonnull String versionString) {
        for (SemanticVersionType type : SemanticVersionType.values()) {
            if (type.isSingleton() && type.getText().equals(versionString)) {
                return SINGLETONS.get(type);
            }
        }

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
        return new SemanticVersion(SemanticVersionType.NORMAL, versionNumbers, prerelease);
    }

    /**
     * Get the type of this version. See the {@link SemanticVersionType} enum for more details.
     *
     * @return the type of this version
     * @see SemanticVersionType
     */
    @Nonnull
    public SemanticVersionType getType() {
        return type;
    }

    @Nonnull
    private static String dotSeparated(@Nonnull String part) {
        return part + "(." + part + ")*";
    }

    @Override
    public String toString() {
        if (type.isSingleton()) {
            return type.getText();
        }
        String version = versionNumbers.stream().map(Object::toString).collect(Collectors.joining("."));
        if (!prerelease.isEmpty()) {
            version = version + "-" + prerelease;
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
        return type.equals(that.type)
                && Objects.equals(versionNumbers, that.versionNumbers)
                && Objects.equals(prerelease, that.prerelease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type.name(), versionNumbers, prerelease);
    }

    @Nonnull
    public List<SemanticVersion> lesserVersions(@Nonnull Collection<SemanticVersion> rawVersions) {
        return rawVersions.stream()
                .filter(other -> other.compareTo(this) < 0)
                .collect(Collectors.toList());
    }

    @Override
    public int compareTo(@Nonnull SemanticVersion o) {
        // First, compare by type using ordinal position. Values in the enum are sorted according
        // to their expected precedence
        int typeCompare = type.compareTo(o.type);
        if (typeCompare != 0) {
            return typeCompare;
        }

        // For singleton values, type comparison is enough.
        if (type.isSingleton()) {
            return 0;
        }

        // Otherwise, compare version numbers and pre-release information
        final int versionComparison = compareVersionNumbers(o);
        if (versionComparison != 0) {
            return versionComparison;
        }
        return comparePrerelease(o);
    }

    private int compareVersionNumbers(@Nonnull SemanticVersion o) {
        // semver only allows 3, we only use 4.
        // gradle supports an arbitrary number, but it gets complicated if there is a pre-release or build info
        if (versionNumbers.size() != o.versionNumbers.size()) {
            throw new IllegalArgumentException("Versions only differ by part count" + this + " vs " + o);
        }
        int length = versionNumbers.size();
        for (int i = 0; i < length; i++) {
            final int result = Integer.compare(versionNumbers.get(i), o.versionNumbers.get(i));
            if (result != 0) {
                return Integer.signum(result);
            }
        }
        return 0;
    }

    private int comparePrerelease(@Nonnull SemanticVersion o) {
        if (!prerelease.isEmpty() && o.prerelease.isEmpty()) {
            return -1;
        } else if (prerelease.isEmpty() && !o.prerelease.isEmpty()) {
            return 1;
        }
        if (!prerelease.equals(o.prerelease)) {
            // We only support `List.of()` and `List.of("SNAPSHOT")`
            throw new IllegalArgumentException("Multiple types of prerelease is not supported");
        }
        return 0;
    }
}
