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
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType.BYTES;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType.LONG;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType.NULL;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType.STRING;

public class KeySpacePathParsingTest {
    private final KeySpace testSpace = getKeySpaceForTesting();

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
                new KeySpaceDirectory("Environment", NULL)
                        .addSubdirectory(new KeySpaceDirectory("App", STRING)
                                .addSubdirectory(new KeySpaceDirectory("User", LONG))));

        final URI expected = URI.create("//testApp/12345");
        final KeySpacePath path = KeySpaceUtils.toKeySpacePath(expected, keySpace);
        Assertions.assertEquals(expected, KeySpaceUtils.pathToUri(path), "KeySpacePath is not parsed as expected");
    }

    @Test
    void testWithNullSubDirectory() throws RelationalException {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("Environment", STRING)
                        .addSubdirectory(new KeySpaceDirectory("App", STRING)
                                .addSubdirectory(new KeySpaceDirectory("User", LONG)))
                        .addSubdirectory(new KeySpaceDirectory("NullApp", NULL)
                                .addSubdirectory(new KeySpaceDirectory("User", LONG))));

        final URI expected = URI.create("/prod/testApp/12345");
        final KeySpacePath path = KeySpaceUtils.toKeySpacePath(expected, keySpace);
        Assertions.assertEquals(expected, KeySpaceUtils.pathToUri(path), "Invalid parsing of URI or KeySpacePaths");

        final URI expected2 = URI.create("/prod//12345");
        final KeySpacePath path2 = KeySpaceUtils.toKeySpacePath(expected2, keySpace);
        Assertions.assertEquals(expected2, KeySpaceUtils.pathToUri(path2), "Invalid parsing of URI or KeySpacePaths");
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
                Pair.of(STRING, "A"),
                Pair.of(LONG, 1L));
    }

    @ParameterizedTest
    @MethodSource("defaultValueSource")
    void defaultValue(Pair<KeySpaceDirectory.KeyType, Object> typeAndDefault) throws RelationalException {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("testRoot", STRING)
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
                new KeySpaceDirectory("testRoot", STRING)
                        .addSubdirectory(new DirectoryLayerDirectory("a", "S")));

        URI uri = URI.create("/prod/S");
        Assertions.assertEquals(uri, KeySpaceUtils.pathToUri(KeySpaceUtils.toKeySpacePath(uri, keySpace)));
        URI wrongUri = URI.create("/prod/3");
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(wrongUri, keySpace))
                .hasErrorCode(ErrorCode.INVALID_PATH);
    }

    @Test
    void unsupportedType() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("testRoot", BYTES));
        RelationalAssertions.assertThrows(
                () -> KeySpaceUtils.toKeySpacePath(URI.create("/foo"), keySpace))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
    }

    private KeySpace getKeySpaceForTesting() {
        return new KeySpace(
                new KeySpaceDirectory("Environment", STRING)
                        .addSubdirectory(new KeySpaceDirectory("App", STRING)
                                .addSubdirectory(new KeySpaceDirectory("User", LONG))));
    }

    private KeySpace getKeySpaceWithDirectoryLayerForTesting() {
        return new KeySpace(
                new DirectoryLayerDirectory("Environment")
                        .addSubdirectory(new DirectoryLayerDirectory("App")
                                .addSubdirectory(new KeySpaceDirectory("User", LONG))));
    }

    private KeySpace sampleKeySpace() {
        return new KeySpace(
                new KeySpaceDirectory("testRoot", STRING, "testRoot")
                        .addSubdirectory(new KeySpaceDirectory("domainId", LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("database")
                                        .addSubdirectory(new KeySpaceDirectory("firstStore", NULL))
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
}
