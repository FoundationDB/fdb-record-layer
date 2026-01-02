/*
 * KeySpacePathParsingTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.recordlayer.util.Hex;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeySpacePathParsingTest {
    private final KeySpace testSpace = getKeySpaceForTesting();

    private static final Set<KeySpaceDirectory.KeyType> PARSEABLE_KEY_TYPES = Set.of(
            KeySpaceDirectory.KeyType.LONG,
            KeySpaceDirectory.KeyType.STRING,
            KeySpaceDirectory.KeyType.NULL);

    private static final UUID RANDOM_UUID = UUID.randomUUID();

    private static final Map<KeySpaceDirectory.KeyType, List<PathEntry>> VALUES_FOR_TYPE = Map.of(
            KeySpaceDirectory.KeyType.BOOLEAN, List.of(
                    new PathEntry("true", true),
                    new PathEntry("false", false)),
            KeySpaceDirectory.KeyType.BYTES, List.of(
                    new PathEntry(Base64.getEncoder().encodeToString(new byte[] {0, 1, 2, 3}), new byte[] {0, 1, 2, 3}),
                    new PathEntry(Hex.encodeHexString(new byte[] {0, 1, 2, 3}), new byte[] {0, 1, 2, 3})),
            KeySpaceDirectory.KeyType.DOUBLE, List.of(
                    new PathEntry("42.43", 42.43),
                    new PathEntry("-50.2", -50.2)),
            KeySpaceDirectory.KeyType.FLOAT, List.of(
                    new PathEntry("42.43", 42.43f),
                    new PathEntry("-50.2", -50.2f)),
            KeySpaceDirectory.KeyType.LONG, List.of(
                    new PathEntry("123987", 123987),
                    new PathEntry("0", 0),
                    new PathEntry("-432", -432)),
            KeySpaceDirectory.KeyType.NULL, List.of(new PathEntry("", null)),
            KeySpaceDirectory.KeyType.STRING, List.of(new PathEntry("foo", "foo")),
            KeySpaceDirectory.KeyType.UUID, List.of(new PathEntry(RANDOM_UUID.toString(), RANDOM_UUID)));

    @Test
    void validateValuesForTypeCoverage() {
        Assertions.assertEquals(Arrays.stream(KeySpaceDirectory.KeyType.values()).collect(Collectors.toSet()),
                VALUES_FOR_TYPE.keySet());
    }

    @Test
    void testParsingKeySpacePath() throws RelationalException {
        URI expected = URI.create("/prod/testApp/12345");
        final URI uri = KeySpaceUtils.pathToUri(KeySpaceUtils.toKeySpacePath(expected, testSpace));
        Assertions.assertEquals(expected, uri, "Invalid parsing of URI or KeySpacePaths");
    }

    @Test
    void cannotParseEmptyUri() {
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(URI.create(""), testSpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @Test
    void testUrlNotValidForKeySpace() {
        //throws the right exception when we can't parse an entry
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(URI.create("/prod/testApp/notAUser"), testSpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @Test
    void testUrlWithEmptyForStringType() {
        // Default keySpace doesn't have directory with null type
        final URI expected = URI.create("//testApp/12345");
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(expected, testSpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @Test
    void testUrlWithDoubleSlashAtBeginning() throws RelationalException {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.NULL)
                        .addSubdirectory(new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG))));

        final URI expected = URI.create("//testApp/12345");
        final KeySpacePath path = KeySpaceUtils.toKeySpacePath(expected, keySpace);
        Assertions.assertEquals(expected, KeySpaceUtils.pathToUri(path), "KeySpacePath is not parsed as expected");
    }

    @Test
    void testWithNullSubDirectory() throws RelationalException {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG)))
                        .addSubdirectory(new KeySpaceDirectory("NullApp", KeySpaceDirectory.KeyType.NULL)
                                .addSubdirectory(new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG))));

        final URI expected = URI.create("/prod/testApp/12345");
        final KeySpacePath path = KeySpaceUtils.toKeySpacePath(expected, keySpace);
        Assertions.assertEquals(expected, KeySpaceUtils.pathToUri(path), "Invalid parsing of URI or KeySpacePaths");

        // Note: this uri is not parseable, because we cannot differentiate between "NullApp", and "App" with an
        // empty string
        final URI expected2 = URI.create("/prod//12345");
        Assertions.assertEquals(expected2, KeySpaceUtils.pathToUri(keySpace.path("Environment", "prod")
                .add("NullApp").add("User", 12345)), "Invalid parsing of URI or KeySpacePaths");
        Assertions.assertEquals(expected2, KeySpaceUtils.pathToUri(keySpace.path("Environment", "prod")
                .add("App", "").add("User", 12345)), "Invalid parsing of URI or KeySpacePaths");
    }

    @Test
    void canParseUris() throws RelationalException {
        /*
         * Explicitly tests that KeySpacePaths can correctly be parsed from URIs
         */
        final KeySpace keySpace = sampleKeySpace();
        KeySpacePath parsedMain = KeySpaceUtils.toKeySpacePath(URI.create("/testRoot/1234/com.apple.test.database/"), keySpace);
        KeySpacePath expectedMain = keySpace.path("testRoot")
                .add("domainId", 1234L)
                .add("database", "com.apple.test.database")
                .add("firstStore");
        Assertions.assertEquals(expectedMain, parsedMain, "Incorrectly parsed the first store path");

        KeySpacePath parsedServer = KeySpaceUtils.toKeySpacePath(URI.create("/testRoot/1234/com.apple.test.database/S"), keySpace);
        KeySpacePath expectedServer = keySpace.path("testRoot")
                .add("domainId", 1234L)
                .add("database", "com.apple.test.database")
                .add("secondStore");
        Assertions.assertEquals(expectedServer, parsedServer, "Incorrectly parsed the second store path");

        KeySpacePath parsedDatabase = KeySpaceUtils.toKeySpacePath(URI.create("/testRoot/1234/com.apple.test.database/C"), keySpace);
        KeySpacePath expectedDatabase = keySpace.path("testRoot")
                .add("domainId", 1234L)
                .add("database", "com.apple.test.database")
                .add("thirdStore");
        Assertions.assertEquals(expectedDatabase, parsedDatabase, "Incorrectly parsed the second store path");
    }

    @Test
    void testDirectoryLayer() throws RelationalException {
        final URI expected = URI.create("/prod/testApp/12345");
        final KeySpacePath path = KeySpaceUtils.toKeySpacePath(expected, getKeySpaceWithDirectoryLayerForTesting());
        final URI uri = KeySpaceUtils.pathToUri(path);
        Assertions.assertEquals(expected, uri, "Invalid parsing of URI or KeySpacePaths");

        // Assert all values for the keySpacePath are Long
        String clusterFile = FDBTestEnvironment.randomClusterFile();
        FDBRecordContext context = FDBDatabaseFactory.instance().getDatabase(clusterFile).openContext();
        List<Object> numbers1 = getResolvedValuesForKeySpacePath(path, context);
        numbers1.stream().forEach(n -> Assertions.assertTrue(n instanceof Long, "Unexpected value type"));

        // Read the resolved values again, and assert again
        context = FDBDatabaseFactory.instance().getDatabase(clusterFile).openContext();
        List<Object> numbers2 = getResolvedValuesForKeySpacePath(path, context);
        numbers2.stream().forEach(n -> Assertions.assertTrue(n instanceof Long, "Unexpected value type"));

        // The values read from different transactions are consistent
        Assertions.assertArrayEquals(numbers1.toArray(), numbers2.toArray(), "Inconsistent resolved values");
    }

    @Test
    void noLeadingSlash() throws RelationalException {
        URI expected = URI.create("/prod/testApp/12345");
        URI input = URI.create("prod/testApp/12345");
        KeySpacePath path = KeySpaceUtils.toKeySpacePath(input, testSpace);
        final URI uri = KeySpaceUtils.pathToUri(path);
        Assertions.assertEquals(expected, uri, "Invalid parsing of URI or KeySpacePaths");
    }

    private static Stream<Pair<KeySpaceDirectory.KeyType, Object>> defaultValueSource() {
        return Stream.of(
                Pair.of(KeySpaceDirectory.KeyType.STRING, "A"),
                Pair.of(KeySpaceDirectory.KeyType.LONG, 1L),
                Pair.of(KeySpaceDirectory.KeyType.LONG, 10));
    }

    @ParameterizedTest
    @MethodSource("defaultValueSource")
    void defaultValue(Pair<KeySpaceDirectory.KeyType, Object> typeAndDefault) throws RelationalException {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("a", typeAndDefault.getLeft(), typeAndDefault.getRight())));

        URI uri = URI.create("/prod/" + typeAndDefault.getRight());
        Assertions.assertEquals(uri, KeySpaceUtils.pathToUri(KeySpaceUtils.toKeySpacePath(uri, keySpace)));
        URI wrongUri = URI.create("/prod/3");
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(wrongUri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @Test
    void defaultValueDirectoryLayer() throws RelationalException {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING)
                        .addSubdirectory(new DirectoryLayerDirectory("a", "S")));

        URI uri = URI.create("/prod/S");
        Assertions.assertEquals(uri, KeySpaceUtils.pathToUri(KeySpaceUtils.toKeySpacePath(uri, keySpace)));
        URI wrongUri = URI.create("/prod/3");
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(wrongUri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    static Stream<Arguments> unsupportedType() {
        return Arrays.stream(KeySpaceDirectory.KeyType.values())
                .filter(type -> !PARSEABLE_KEY_TYPES.contains(type))
                .map(type -> Arguments.of(type, VALUES_FOR_TYPE.get(type).get(0)));
    }

    @ParameterizedTest
    @MethodSource
    void unsupportedType(KeySpaceDirectory.KeyType type, PathEntry entry) {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("testRoot", type));
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(URI.create("/" + entry.uriEntry), keySpace))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
    }

    static Stream<Arguments> supportedType() {
        return Arrays.stream(KeySpaceDirectory.KeyType.values())
                .filter(PARSEABLE_KEY_TYPES::contains)
                .flatMap(type -> ParameterizedTestUtils.booleans("constant")
                        .flatMap(constant -> VALUES_FOR_TYPE.get(type).stream()
                                .map(pathEntry -> Arguments.of(type, constant, pathEntry))));
    }

    @ParameterizedTest
    @MethodSource
    void supportedType(KeySpaceDirectory.KeyType type, boolean constant, PathEntry entry) throws RelationalException {
        KeySpace keySpace = new KeySpace(
                createDirectory(type.name(), type, constant, entry.pathEntry));
        Assertions.assertEquals(keySpace.path(type.name(), entry.pathEntry),
                KeySpaceUtils.toKeySpacePath(URI.create("/" + entry.uriEntry), keySpace));
    }

    @ParameterizedTest
    @BooleanSource({"constant", "directory"})
    void emptyString(boolean constant, boolean directory) {
        // we don't allow empty strings
        final KeySpaceDirectory root = new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING)
                .addSubdirectory(createStringLikeDirectory("STRING", directory, constant, ""));
        KeySpace keySpace = new KeySpace(root);
        final URI uri = URI.create("/root/");
        RelationalAssertions.assertThrows(
                        () -> KeySpaceUtils.toKeySpacePath(uri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @ParameterizedTest
    @BooleanSource({"constant", "directory"})
    void emptyStringMiddle(boolean constant, boolean directory) {
        // we don't allow empty strings
        final KeySpaceDirectory root = new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING)
                .addSubdirectory(createStringLikeDirectory("STRING", directory, constant, "")
                        .addSubdirectory(createStringLikeDirectory("STRING2", directory, constant, "Y")));
        KeySpace keySpace = new KeySpace(root);
        final URI uri = URI.create("/root//Y");
        RelationalAssertions.assertThrows(
                        () -> KeySpaceUtils.toKeySpacePath(uri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @Test
    void longerUri() throws RelationalException {
        // we don't allow empty strings
        final KeySpaceDirectory root = new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING)
                .addSubdirectory(createStringLikeDirectory("STRING", false, false, ""));
        KeySpace keySpace = new KeySpace(root);
        final URI okUri = URI.create("/root/x");
        Assertions.assertEquals(keySpace.path("testRoot", "root").add("STRING", "x"),
                KeySpaceUtils.toKeySpacePath(okUri, keySpace));
        final URI longerUri = URI.create("/root/x/y");
        RelationalAssertions.assertThrows(
                        () -> KeySpaceUtils.toKeySpacePath(longerUri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    static Stream<Arguments> ambiguousScenarios() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(
                        new AmbiguousScenario("/banana",
                                directoryAmbiguousHalf("banana"),
                                ambiguousHalf(KeySpaceDirectory.KeyType.STRING, "banana")),
                        new AmbiguousScenario("/12345",
                                ambiguousHalf(KeySpaceDirectory.KeyType.STRING, "12345"),
                                ambiguousHalf(KeySpaceDirectory.KeyType.LONG, 12345)),
                        new AmbiguousScenario("/",
                                ambiguousHalf(KeySpaceDirectory.KeyType.STRING, ""),
                                ambiguousHalf(KeySpaceDirectory.KeyType.NULL, null)),
                        new AmbiguousScenario("/",
                                directoryAmbiguousHalf(""),
                                ambiguousHalf(KeySpaceDirectory.KeyType.NULL, null))

                ).flatMap(scenario -> Stream.of(scenario, scenario.reversed())),
                ParameterizedTestUtils.booleans("firstIsConstant"),
                ParameterizedTestUtils.booleans("secondIsConstant")
        );
    }

    @ParameterizedTest
    @MethodSource("ambiguousScenarios")
    void ambiguousScenariosAtRoot(AmbiguousScenario scenario, boolean firstIsConstant, boolean secondIsConstant) throws RelationalException {
        final KeySpaceDirectory firstDirectory = scenario.firstHalf.createDirectory.apply(firstIsConstant);
        final KeySpaceDirectory secondDirectory = scenario.secondHalf.createDirectory.apply(secondIsConstant);
        Assumptions.assumeFalse(secondDirectory.getKeyType() == KeySpaceDirectory.KeyType.NULL,
                "empty strings are not allowed regardless of siblings");
        Assumptions.assumeFalse(firstDirectory.getKeyType() == KeySpaceDirectory.KeyType.NULL,
                "null is not allowed at the root");
        KeySpace keySpace = new KeySpace(firstDirectory);
        final URI uri = URI.create(scenario.uri);
        Assertions.assertEquals(scenario.firstHalf.createPath.apply(keySpace),
                KeySpaceUtils.toKeySpacePath(uri, keySpace));

        // now that there is an ambiguous entry with a directory layer it should fail
        keySpace.getRoot().addSubdirectory(secondDirectory);
        RelationalAssertions.assertThrows(() -> KeySpaceUtils.toKeySpacePath(uri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @ParameterizedTest
    @MethodSource("ambiguousScenarios")
    void ambiguousScenarios(AmbiguousScenario scenario, boolean firstIsConstant, boolean secondIsConstant) throws RelationalException {
        final KeySpaceDirectory secondDirectory = scenario.secondHalf.createDirectory.apply(secondIsConstant);
        Assumptions.assumeFalse(secondDirectory.getKeyType() == KeySpaceDirectory.KeyType.NULL,
                "Empty strings are not allowed regardless of siblings");
        final KeySpaceDirectory root = new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING)
                .addSubdirectory(scenario.firstHalf.createDirectory.apply(firstIsConstant));
        KeySpace keySpace = new KeySpace(root);
        final URI uri = URI.create("/root" + scenario.uri);
        Assertions.assertEquals(scenario.firstHalf.addToPath.apply(keySpace.path("testRoot", "root")),
                KeySpaceUtils.toKeySpacePath(uri, keySpace));

        // now that there is an ambiguous entry with a directory layer it should fail
        root.addSubdirectory(secondDirectory);
        RelationalAssertions.assertThrows(() -> KeySpaceUtils.toKeySpacePath(uri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }


    @Nonnull
    private static KeySpaceDirectory createStringLikeDirectory(String name, final boolean directory, final boolean constant, String constantValue) {
        if (directory) {
            return createDirectoryLayerDirectory(name, constant, constantValue);
        } else {
            return createDirectory(name, KeySpaceDirectory.KeyType.STRING, constant, constantValue);
        }
    }

    @Nonnull
    private static KeySpaceDirectory createDirectoryLayerDirectory(String name, final boolean constant, String constantValue) {
        if (constant) {
            return new DirectoryLayerDirectory(name, constantValue);
        } else {
            return new DirectoryLayerDirectory(name);
        }
    }

    @Nonnull
    private static KeySpaceDirectory createDirectory(String name, KeySpaceDirectory.KeyType type,
                                                     boolean constant, Object constantValue) {
        if (constant) {
            return new KeySpaceDirectory(name, type, constantValue);
        } else {
            return new KeySpaceDirectory(name, type);
        }
    }

    private KeySpace getKeySpaceForTesting() {
        return new KeySpace(
                new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG))));
    }

    private KeySpace getKeySpaceWithDirectoryLayerForTesting() {
        return new KeySpace(
                new DirectoryLayerDirectory("Environment")
                        .addSubdirectory(new DirectoryLayerDirectory("App")
                                .addSubdirectory(new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG))));
    }

    private KeySpace sampleKeySpace() {
        return new KeySpace(
                new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING, "testRoot")
                        .addSubdirectory(new KeySpaceDirectory("domainId", KeySpaceDirectory.KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("database")
                                        .addSubdirectory(new KeySpaceDirectory("firstStore", KeySpaceDirectory.KeyType.NULL))
                                        .addSubdirectory(new DirectoryLayerDirectory("secondStore", "S"))
                                        .addSubdirectory(new DirectoryLayerDirectory("thirdStore", "C"))
                                )
                        )
        );
    }

    private List<Object> getResolvedValuesForKeySpacePath(@Nonnull KeySpacePath path, @Nonnull FDBRecordContext context) throws RelationalException {
        try {
            List<Object> values = new ArrayList<>();
            KeySpacePath currentPath = path;
            values.add(context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, currentPath.resolveAsync(context)).getResolvedValue());
            while (currentPath.getParent() != null) {
                currentPath = currentPath.getParent();
                values.add(context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, currentPath.resolveAsync(context)).getResolvedValue());
            }
            return values;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    private static final class PathEntry {
        @Nonnull
        private final String uriEntry;
        @Nullable
        private final Object pathEntry;

        private PathEntry(@Nonnull final String uriEntry, @Nullable final Object pathEntry) {
            this.uriEntry = uriEntry;
            this.pathEntry = pathEntry;
        }

        @Override
        public String toString() {
            return uriEntry;
        }
    }

    @Nonnull
    private static AmbiguousHalf directoryAmbiguousHalf(String value) {
        final String name = "DirectoryLayer";
        return new AmbiguousHalf(name,
                isConstant -> createDirectoryLayerDirectory(name, isConstant, value),
                keySpace -> keySpace.path(name, value),
                path -> path.add(name, value));
    }

    static AmbiguousHalf ambiguousHalf(final KeySpaceDirectory.KeyType type, final Object value) {
        return new AmbiguousHalf(type.name(), isConstant -> createDirectory(type.name(), type, isConstant, value),
                keySpace -> keySpace.path(type.name(), value),
                path -> path.add(type.name(), value));
    }

    private static class AmbiguousHalf {
        private final String name;
        private final Function<Boolean, KeySpaceDirectory> createDirectory;
        private final Function<KeySpace, KeySpacePath> createPath;
        private final Function<KeySpacePath, KeySpacePath> addToPath;

        private AmbiguousHalf(final String name, final Function<Boolean, KeySpaceDirectory> createDirectory, final Function<KeySpace, KeySpacePath> createPath, final Function<KeySpacePath, KeySpacePath> addToPath) {
            this.name = name;
            this.createDirectory = createDirectory;
            this.createPath = createPath;
            this.addToPath = addToPath;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class AmbiguousScenario {
        private final String uri;
        private final AmbiguousHalf firstHalf;
        private final AmbiguousHalf secondHalf;

        public AmbiguousScenario(final String uri,
                                 final AmbiguousHalf firstHalf, final AmbiguousHalf secondHalf) {
            this.uri = uri;
            this.firstHalf = firstHalf;
            this.secondHalf = secondHalf;
        }

        public AmbiguousScenario reversed() {
            return new AmbiguousScenario(uri, secondHalf, firstHalf);
        }

        @Override
        public String toString() {
            return firstHalf + " and " + secondHalf;
        }
    }
}
