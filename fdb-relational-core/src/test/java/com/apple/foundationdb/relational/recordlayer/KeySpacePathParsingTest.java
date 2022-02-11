/*
 * KeySpacePathParsingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

public class KeySpacePathParsingTest {
    private final KeySpace testSpace = getKeySpaceForTesting();

    @Test
    void testParsingKeySpacePath() throws RelationalException {
        URI expected = URI.create("/prod/testApp/12345");
        KeySpacePath path = KeySpaceUtils.uriToPath(expected, testSpace);
        final URI uri = KeySpaceUtils.pathToUri(path);
        Assertions.assertEquals(expected, uri, "Invalid parsing of URI or KeySpacePaths");
    }

    @Test
    void cannotParseEmptyUri() {
        RelationalAssertions.assertThrowsRelationalException(
                () -> KeySpaceUtils.uriToPath(URI.create(""), testSpace),
                ErrorCode.INVALID_PATH);
    }

    @Test
    void testUrlNotValidForKeySpace() {
        //throws the right exception when we can't parse an entry
        RelationalAssertions.assertThrowsRelationalException(
                () -> KeySpaceUtils.uriToPath(URI.create("/prod/testApp/notAUser"), testSpace),
                ErrorCode.INVALID_PATH);
    }

    @Test
    void testUrlWithEmptyForStringType() {
        // Default keySpace doesn't have directory with null type
        final URI expected = URI.create("//testApp/12345");
        RelationalAssertions.assertThrowsRelationalException(
                () -> KeySpaceUtils.uriToPath(expected, testSpace),
                ErrorCode.INVALID_PATH);
    }

    @Test
    void testUrlWithDoubleSlashAtBeginning() throws RelationalException {
        final KeySpaceDirectory env = new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.NULL);
        final KeySpaceDirectory app = new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory user = new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG);
        env.addSubdirectory(app);
        app.addSubdirectory(user);
        KeySpace keySpace = new KeySpace(env);

        final URI expected = URI.create("//testApp/12345");
        final KeySpacePath path = KeySpaceUtils.uriToPath(expected, keySpace);
        Assertions.assertEquals(expected, KeySpaceUtils.pathToUri(path), "KeySpacePath is not parsed as expected");
    }

    @Test
    void testWithNullSubDirectory() throws RelationalException {
        final KeySpaceDirectory env = new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory app = new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory nullApp = new KeySpaceDirectory("NullApp", KeySpaceDirectory.KeyType.NULL);
        final KeySpaceDirectory user = new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG);
        final KeySpaceDirectory user2 = new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG);
        env.addSubdirectory(app);
        env.addSubdirectory(nullApp);
        app.addSubdirectory(user);
        nullApp.addSubdirectory(user2);
        final KeySpace keySpace = new KeySpace(env);

        final URI expected = URI.create("/prod/testApp/12345");
        final KeySpacePath path = KeySpaceUtils.uriToPath(expected, keySpace);
        Assertions.assertEquals(expected, KeySpaceUtils.pathToUri(path), "Invalid parsing of URI or KeySpacePaths");

        final URI expected2 = URI.create("/prod//12345");
        final KeySpacePath path2 = KeySpaceUtils.uriToPath(expected2, keySpace);
        Assertions.assertEquals(expected2, KeySpaceUtils.pathToUri(path2), "Invalid parsing of URI or KeySpacePaths");
    }

    @Test
    void testKeySpaceForSchemaExtension() throws RelationalException {
        final KeySpaceDirectory env = new KeySpaceDirectory("env", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory db = new KeySpaceDirectory("db", KeySpaceDirectory.KeyType.STRING);
        env.addSubdirectory(db);
        KeySpace keySpace = new KeySpace(env);

        KeySpacePath dbPath = KeySpaceUtils.uriToPath(URI.create("/prod/testDb"), keySpace);
        KeySpace extended = KeySpaceUtils.extendKeySpaceForSchema(keySpace, dbPath, "testSchema");
        String keySpaceString = extended.toString();
        Assertions.assertEquals("/ (NULL)\n    +- env (STRING)\n       +- db (STRING)\n          +- testSchema (STRING=testSchema)\n", keySpaceString);

        KeySpaceDirectory schemaDirectory = extended.getDirectory("env").getSubdirectory("db").getSubdirectory("testSchema");
        Assertions.assertEquals("testSchema", schemaDirectory.getValue());
    }

    @Test
    void testSchemaInKeySpaceNotToBeOverridden() throws RelationalException {
        final KeySpaceDirectory env = new KeySpaceDirectory("env", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory db = new KeySpaceDirectory("db", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory schema = new KeySpaceDirectory("testSchema", KeySpaceDirectory.KeyType.STRING, "T");
        db.addSubdirectory(schema);
        env.addSubdirectory(db);
        KeySpace keySpace = new KeySpace(env);

        KeySpacePath dbPath = KeySpaceUtils.uriToPath(URI.create("/prod/testDb"), keySpace);
        KeySpace extended = KeySpaceUtils.extendKeySpaceForSchema(keySpace, dbPath, "testSchema");
        KeySpaceDirectory schemaDirectory = extended.getDirectory("env").getSubdirectory("db").getSubdirectory("testSchema");
        Assertions.assertEquals("T", schemaDirectory.getValue());
    }

    @Test
    void canParseUris() throws RelationalException {
        /*
         * Explicitly tests that KeySpacePaths can correctly be parsed from URIs
         */
        final KeySpace keySpace = sampleKeySpace();
        KeySpacePath parsedMain = KeySpaceUtils.uriToPath(URI.create("/testRoot/1234/1/"), keySpace);
        KeySpacePath expectedMain = keySpace.path("testRoot").add("domainId", 1234L).add("database", 1L).add("firstStore");
        Assertions.assertEquals(expectedMain, parsedMain, "Incorrectly parsed the first store path");

        KeySpacePath parsedServer = KeySpaceUtils.uriToPath(URI.create("/testRoot/1234/1/1"), keySpace);
        KeySpacePath expectedServer = keySpace.path("testRoot").add("domainId", 1234L).add("database", 1L).add("secondStore", 1L);
        Assertions.assertEquals(expectedServer, parsedServer, "Incorrectly parsed the second store path");

        KeySpacePath parsedDatabase = KeySpaceUtils.uriToPath(URI.create("/testRoot/1234/1/2"), keySpace);
        KeySpacePath expectedDatabase = keySpace.path("testRoot").add("domainId", 1234L).add("database", 1L).add("thirdStore", 2L);
        Assertions.assertEquals(expectedDatabase, parsedDatabase, "Incorrectly parsed the second store path");
    }

    @Test
    void testDirectoryLayer() throws RelationalException {
        final URI expected = URI.create("/prod/testApp/12345");
        final KeySpacePath path = KeySpaceUtils.uriToPath(expected, getKeySpaceWithDirectoryLayerForTesting());
        final URI uri = KeySpaceUtils.pathToUri(path);
        Assertions.assertEquals(expected, uri, "Invalid parsing of URI or KeySpacePaths");

        // Assert all values for the keySpacePath are Long
        FDBRecordContext context = FDBDatabaseFactory.instance().getDatabase().openContext();
        List<Object> numbers1 = getResolvedValuesForKeySpacePath(path, context);
        numbers1.stream().forEach(n -> Assertions.assertTrue(n instanceof Long, "Unexpected value type"));

        // Read the resolved values again, and assert again
        context = FDBDatabaseFactory.instance().getDatabase().openContext();
        List<Object> numbers2 = getResolvedValuesForKeySpacePath(path, context);
        numbers2.stream().forEach(n -> Assertions.assertTrue(n instanceof Long, "Unexpected value type"));

        // The values read from different transactions are consistent
        Assertions.assertArrayEquals(numbers1.toArray(), numbers2.toArray(), "Inconsistent resolved values");
    }

    private KeySpace getKeySpaceForTesting() {
        final KeySpaceDirectory env = new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory app = new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory user = new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG);
        env.addSubdirectory(app);
        app.addSubdirectory(user);
        return new KeySpace(env);
    }

    private KeySpace getKeySpaceWithDirectoryLayerForTesting() {
        return new KeySpace(
                new DirectoryLayerDirectory("Environment")
                        .addSubdirectory(new DirectoryLayerDirectory("App")
                                .addSubdirectory(new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG))));
    }

    private KeySpace sampleKeySpace() {
        final KeySpaceDirectory ds1 = new KeySpaceDirectory("domainId", KeySpaceDirectory.KeyType.LONG);
        final KeySpaceDirectory database1 = new KeySpaceDirectory("database", KeySpaceDirectory.KeyType.LONG);
        final KeySpaceDirectory main = new KeySpaceDirectory("firstStore", KeySpaceDirectory.KeyType.NULL);
        // in reality, the following items are actually DirectoryLayerDirectory instances; since we don't really want to
        // mess with a DirectoryLayer in these tests, we just use fake mappings:
        // "S" = 1
        // "C" = 2
        // "DSY" = 3
        final KeySpaceDirectory server = new KeySpaceDirectory("secondStore", KeySpaceDirectory.KeyType.LONG, 1L);
        final KeySpaceDirectory databaseStore = new KeySpaceDirectory("thirdStore", KeySpaceDirectory.KeyType.LONG, 2L);
        final KeySpaceDirectory fourthStore = new KeySpaceDirectory("fourthStore", KeySpaceDirectory.KeyType.LONG, 3L);
        ds1.addSubdirectory(database1);
        database1.addSubdirectory(server).addSubdirectory(main).addSubdirectory(databaseStore).addSubdirectory(fourthStore);

        final KeySpaceDirectory base = new KeySpaceDirectory("testRoot", KeySpaceDirectory.KeyType.STRING, "testRoot");
        base.addSubdirectory(ds1);
        return new KeySpace(base);
    }

    private List<Object> getResolvedValuesForKeySpacePath(@Nonnull KeySpacePath path, @Nonnull FDBRecordContext context) {
        List<Object> values = new ArrayList<>();
        KeySpacePath currentPath = path;
        values.add(context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, currentPath.resolveAsync(context)).getResolvedValue());
        while (currentPath.getParent() != null) {
            currentPath = currentPath.getParent();
            values.add(context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, currentPath.resolveAsync(context)).getResolvedValue());
        }
        return values;
    }
}
