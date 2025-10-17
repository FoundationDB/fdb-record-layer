/*
 * KeySpaceDirectoryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.ValueRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.util.RandomUtil;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static com.apple.foundationdb.record.TestHelpers.eventually;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.DEFAULT_CHECK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeySpaceDirectory}.
 */
@Tag(Tags.RequiresFDB)
public class KeySpaceDirectoryTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    private static class KeyTypeValue {
        KeyType keyType;
        @Nullable
        Object value;
        @Nullable
        Object value2;
        Supplier<Object> generator;

        public KeyTypeValue(KeyType keyType, @Nullable Object value, @Nullable Object value2, Supplier<Object> generator) {
            this.keyType = keyType;
            this.value = value;
            this.value2 = value2;
            this.generator = generator;
            assertTrue(keyType.isMatch(value));
            assertTrue(keyType.isMatch(generator.get()));
        }

        @Override
        public String toString() {
            return "KeyTypeValue{" + keyType + '}';
        }
    }

    private static final Random random = new Random();

    private static final List<KeyTypeValue> valueOfEveryType = new ImmutableList.Builder<KeyTypeValue>()
            .add(new KeyTypeValue(KeyType.NULL, null, null, () -> null))
            .add(new KeyTypeValue(KeyType.BYTES, new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04 }, () -> {
                int size = random.nextInt(10) + 1;
                byte[] bytes = new byte[size];
                random.nextBytes(bytes);
                return bytes;
            }))
            .add(new KeyTypeValue(KeyType.STRING, "hello", "goodbye", () -> RandomUtil.randomAlphanumericString(random, random.nextInt(10) + 1)))
            .add(new KeyTypeValue(KeyType.LONG, 11L,  -11L, random::nextLong))
            .add(new KeyTypeValue(KeyType.FLOAT, 3.2f, -5.4f, random::nextFloat))
            .add(new KeyTypeValue(KeyType.DOUBLE, 9.7d, -3845.6d, random::nextDouble))
            .add(new KeyTypeValue(KeyType.BOOLEAN, true, false, random::nextBoolean))
            .add(new KeyTypeValue(KeyType.UUID, UUID.randomUUID(), UUID.randomUUID(), UUID::randomUUID))
            .build();

    // Catch if someone adds a new type to make sure that we account for it in this test harness
    @Test
    public void testValueOfEveryTypeReallyIsEveryType() {
        List<KeyType> keyTypes = Lists.newArrayList(KeyType.values());
        Iterator<KeyType> iter = keyTypes.iterator();
        while (iter.hasNext()) {
            KeyType keyType = iter.next();
            for (KeyTypeValue value : valueOfEveryType) {
                if (value.keyType == keyType) {
                    iter.remove();
                    break;
                }
            }
        }
        assertTrue(keyTypes.isEmpty(), "A new type has been added that is not being tested: " + keyTypes);
    }

    @Test
    public void testRestrictSubdirDuplicateName() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.STRING),
                        new KeySpaceDirectory("root1", KeyType.LONG)));
    }

    @Test
    public void testRestrictSubdirDuplicateType() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.STRING),
                        new KeySpaceDirectory("root2", KeyType.STRING)));
    }

    @Test
    public void testAllowDifferentConstantValueOfSameType() {
        new KeySpace(
                new KeySpaceDirectory("root1",  KeyType.STRING, "production"),
                new KeySpaceDirectory("root2",  KeyType.STRING, "test"));
    }

    @Test
    public void testRestrictSameConstantValueOfSameType() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.STRING, "production"),
                        new KeySpaceDirectory("root1", KeyType.STRING, "production")));
    }

    @Test
    public void testRestrictAnyLongAndDirectoryLayerLong() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG))));

        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1",  "A"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG))));
    }

    @Test
    public void testRestrictConstantLongAndDirectoryLayerLong() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG, 10L))));

        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG, 10L))));
    }

    @Test
    public void testRestrictAnyDirectoryLayerAndConstantDirectoryLayer() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                                .addSubdirectory(new DirectoryLayerDirectory("dir2"))));
    }

    @Test
    public void testBadDirectoryLayerTypes() throws Exception {
        for (KeyTypeValue keyTypeValue : valueOfEveryType) {
            if (! (keyTypeValue.keyType == KeyType.STRING)) {
                assertThrows(RecordCoreArgumentException.class, () ->
                        new KeySpace(new DirectoryLayerDirectory("root", keyTypeValue.value)));
            } else {
                new KeySpace(new DirectoryLayerDirectory("root", keyTypeValue.value));
            }
        }
    }

    @Test
    public void testRestrictSameConstantDirectoryLayer() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                                .addSubdirectory(new DirectoryLayerDirectory("dir2", "A"))));
    }

    @Test
    public void testAllowDifferentConstantDirectoryLayer() {
        new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "production")
                        .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                        .addSubdirectory(new DirectoryLayerDirectory("dir2", "B")));
    }

    @Test
    public void testRestrictConstantAndAnyValueOfSameType() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.LONG, 1L),
                        new KeySpaceDirectory("root2", KeyType.LONG)));
    }

    @Test
    public void testBadConstantForType() throws Exception {
        for (KeyType keyType : KeyType.values()) {
            for (KeyTypeValue keyTypeValue : valueOfEveryType) {
                if (! (keyType == keyTypeValue.keyType)) {
                    assertThrows(RecordCoreArgumentException.class, () ->
                            new KeySpace(new KeySpaceDirectory("root", keyType, keyTypeValue.value)));
                } else {
                    new KeySpace(new KeySpaceDirectory("root", keyType, keyTypeValue.value));
                }
            }
        }
    }

    @ParameterizedTest(name = "testPathToAndFromTuple[clearCaches={0}]")
    @BooleanSource
    public void testPathToAndFromTuple(boolean clearCaches) {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("production", "production")
                        .addSubdirectory(new KeySpaceDirectory("userid", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("application")
                                                .addSubdirectory(new KeySpaceDirectory("dataStore", KeyType.NULL))
                                                .addSubdirectory(new DirectoryLayerDirectory("metadataStore", "S")))),
                new DirectoryLayerDirectory("test", "test")
                        .addSubdirectory(new KeySpaceDirectory("userid", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("application")
                                        .addSubdirectory(new KeySpaceDirectory("dataStore", KeyType.NULL))
                                        .addSubdirectory(new DirectoryLayerDirectory("metadataStore", "S")))));

        final KeySpacePath path1 = root.path("production")
                .add("userid", 123456789L)
                .add("application", "com.mybiz.application1")
                .add("dataStore");

        final KeySpacePath path2 = root.path("test")
                .add("userid", 987654321L)
                .add("application", "com.mybiz.application2")
                .add("metadataStore");

        final FDBDatabase database = dbExtension.getDatabase();
        final Tuple path1Tuple;
        final Tuple path2Tuple;
        try (FDBRecordContext context = database.openContext()) {
            path1Tuple = path1.toTuple(context);
            path2Tuple = path2.toTuple(context);

            if (clearCaches) {
                database.clearReverseDirectoryCache();
            }

            // Check resolving with the same transaction that created the entry
            assertResolvesFromKey(context, path1Tuple, root,
                    "production", "production",
                    "userid", 123456789L,
                    "application", "com.mybiz.application1",
                    "dataStore", null);

            assertResolvesFromKey(context, path2Tuple, root,
                    "test", "test",
                    "userid", 987654321L,
                    "application", "com.mybiz.application2",
                    "metadataStore", "S");

            context.commit();
        }

        final Tuple path1ExpectedTuple;
        final Tuple path2ExpectedTuple;
        try (FDBRecordContext context = database.openContext()) {
            List<Long> entries = resolveBatch(context, "production", "test",
                    "com.mybiz.application1", "com.mybiz.application2", "S");
            path1ExpectedTuple = Tuple.from(entries.get(0), 123456789L, entries.get(2), null);
            path2ExpectedTuple = Tuple.from(entries.get(1), 987654321L, entries.get(3), entries.get(4));

            if (clearCaches) {
                database.clearReverseDirectoryCache();
            }

            assertEquals(path1ExpectedTuple, path1Tuple);
            assertEquals(path2ExpectedTuple, path2Tuple);

            // Now, make sure that we can take a tuple and turn it back into a keyspace path.
            assertResolvesFromKey(context, path1ExpectedTuple, root,
                    "production", "production",
                    "userid", 123456789L,
                    "application", "com.mybiz.application1",
                    "dataStore", null);

            // Tack on extra value to make sure it is in the remainder.
            Tuple extendedPath2 = path2ExpectedTuple.add(10L);
            ResolvedKeySpacePath revPath2 = assertResolvesFromKey(context, extendedPath2, root,
                    "test", "test",
                    "userid", 987654321L,
                    "application", "com.mybiz.application2",
                    "metadataStore", "S");
            assertEquals(Tuple.from(10L), revPath2.getRemainder());
        }
    }

    private ResolvedKeySpacePath assertResolvesFromKey(FDBRecordContext context, Tuple t, KeySpace keySpace, Object... dirPath) {
        List<Pair<String, Object>> dirPathList = new ArrayList<>(dirPath.length / 2);
        for (int i = 0; i < dirPath.length; i += 2) {
            assertThat(dirPath[i], instanceOf(String.class));
            String directory = (String) dirPath[i];
            assertThat(i + 1, lessThan(dirPath.length));
            dirPathList.add(Pair.of(directory, dirPath[i + 1]));
        }
        return assertResolvesFromKey(context, t, keySpace, dirPathList);
    }

    private ResolvedKeySpacePath assertResolvesFromKey(FDBRecordContext context, Tuple t, KeySpace keySpace, List<Pair<String, Object>> dirPath) {
        ResolvedKeySpacePath resolved = keySpace.resolveFromKey(context, t);
        List<ResolvedKeySpacePath> flattened = keySpace.resolveFromKey(context, t).flatten();
        assertEquals(dirPath.size(), flattened.size());

        for (int i = 0; i < dirPath.size(); i++) {
            ResolvedKeySpacePath resolvedElem = flattened.get(i);
            Pair<String, Object> pathElem = dirPath.get(i);
            assertEquals(resolvedElem.getDirectoryName(), pathElem.getLeft());
            assertEquals(resolvedElem.getLogicalValue(), pathElem.getRight());
            assertEquals(resolvedElem.getResolvedValue(), t.get(i));
        }

        return resolved;
    }

    @Test
    public void testInvalidPath() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root",  KeyType.STRING, "production")
                        .addSubdirectory(new KeySpaceDirectory("a", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            // Building a tuple in the correct order works
            Tuple tuple = root.path("root")
                    .add("a", "foo")
                    .add("b", "bar").toTuple(context);
            assertEquals(Tuple.from("production", "foo", "bar"), tuple);

            // Walking in the wrong order fails
            assertThrows(NoSuchDirectoryException.class,
                    () -> root.path("foo").add("a", "bar").toTuple(context));
        }
    }

    @Test
    public void testAllTypesAnyValues() throws Exception {
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.LONG, 1L);
        for (KeyTypeValue kv : valueOfEveryType) {
            rootDir.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType));
        }

        KeySpace root = new KeySpace(rootDir);

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {

            for (KeyTypeValue kv : valueOfEveryType) {
                // Test that we can set a good type and get the right tuple back
                final Object value = kv.generator.get();
                assertEquals(Tuple.from(1L, value),
                        root.path("root").add(kv.keyType.toString(), value).toTuple(context));

                final Object badValue = pickDifferentType(kv.keyType).generator.get();
                assertThrows(RecordCoreArgumentException.class,
                        () -> root.path("root").add(kv.keyType.toString(), badValue).toTuple(context));
            }
        }
    }

    public KeyTypeValue pickDifferentType(KeyType keyType) {
        while (true) {
            KeyTypeValue kv = valueOfEveryType.get(random.nextInt(valueOfEveryType.size()));
            if (kv.keyType != keyType) {
                return kv;
            }
        }
    }

    @Test
    public void testAllTypesConstValues() throws Exception {
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.LONG, 1L);
        for (KeyTypeValue kv : valueOfEveryType) {
            rootDir.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType, kv.value));
        }

        KeySpace root = new KeySpace(rootDir);

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            // Test all constants that match the ones we created with
            for (KeyTypeValue kv : valueOfEveryType) {
                assertEquals(Tuple.from(1L, kv.value),
                        root.path("root").add(kv.keyType.toString()).toTuple(context));
                assertEquals(Tuple.from(1L, kv.value),
                        root.path("root").add(kv.keyType.toString(), kv.value).toTuple(context));

                // Try a different value of the same type and make sure that fails
                if (kv.keyType != KeyType.NULL) {
                    assertThrows(RecordCoreArgumentException.class,
                            () -> root.path("root").add(kv.keyType.toString(), kv.value2).toTuple(context));
                }

                // Try a completely different type and make sure that fails
                final Object badValue = pickDifferentType(kv.keyType).generator.get();
                assertThrows(RecordCoreArgumentException.class,
                        () -> root.path("root").add(kv.keyType.toString(), badValue).toTuple(context));
            }
        }

    }

    @Test
    public void testDirectoryLayerDirectoryUsingLongs() {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("cabinet", "cabinet")
                        .addSubdirectory(new DirectoryLayerDirectory("game")));
        final FDBDatabase database = dbExtension.getDatabase();
        final Tuple senetTuple;
        final Tuple urTuple;
        try (FDBRecordContext context = database.openContext()) {
            senetTuple = root.path("cabinet").add("game", "senet").toTuple(context);
            urTuple = root.path("cabinet").add("game", "royal_game_of_ur").toTuple(context);
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            // Verify that I can create the tuple again using the directory layer values.
            assertEquals(senetTuple, root.path("cabinet")
                    .add("game", senetTuple.getLong(1)).toTuple(context));
            assertEquals(urTuple, root.path("cabinet")
                    .add("game", urTuple.getLong(1)).toTuple(context));
        }
    }

    @Test
    public void testDirectoryLayerDirectoryValidation() throws Exception {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("school", "school")
                        .addSubdirectory(new DirectoryLayerDirectory("school_name")
                            .addSubdirectory(new DirectoryLayerDirectory("teachers", "teachers"))
                            .addSubdirectory(new DirectoryLayerDirectory("students", "students"))));
        final FDBDatabase database = dbExtension.getDatabase();
        final Tuple teachersTuple;
        final Tuple studentsTuple;
        try (FDBRecordContext context = database.openContext()) {
            teachersTuple = root.path("school").add("school_name", "Football Tech").add("teachers").toTuple(context);
            studentsTuple = root.path("school").add("school_name", "Football Tech").add("students").toTuple(context);
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            // Use the wrong directory layer value for the students and teachers
            assertThrows(RecordCoreArgumentException.class,
                    () -> root.path("school")
                            .add("school_name", "Football Tech")
                            .add("teachers", studentsTuple.getLong(1)).toTuple(context));
            assertThrows(RecordCoreArgumentException.class,
                    () -> root.path("school")
                            .add("school_name", "Football Tech")
                            .add("students", teachersTuple.getLong(1)).toTuple(context));

            // Use a value that does not exist in the directory layer as the school name.
            assertThrows(NoSuchElementException.class,
                    () -> root.path("school")
                            .add("school_name", -746464638L)
                            .add("teachers").toTuple(context));
        }
    }

    @Test
    public void testDirectoryLayerDirectoryWithMetadata() {
        String testRoot = "test-root-" + random.nextInt();
        ResolverCreateHooks hooks = new ResolverCreateHooks(DEFAULT_CHECK, DirWithMetadataWrapper::metadataHook);
        KeySpace root = rootForMetadataTests(testRoot, hooks, getGenerator());
        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            String dir1 = "test-string-" + random.nextInt();
            String dir2 = "test-string-" + random.nextInt();
            KeySpacePath path1 = root.path(testRoot).add("dir_with_metadata_name", dir1);
            KeySpacePath path2 = root.path(testRoot).add("dir_with_metadata_name", dir2);

            assertThat("path gets wrapped", path1, is(instanceOf(DirWithMetadataWrapper.class)));
            assertThat("path gets wrapped", path2, is(instanceOf(DirWithMetadataWrapper.class)));
            DirWithMetadataWrapper wrapped1 = (DirWithMetadataWrapper) path1;
            DirWithMetadataWrapper wrapped2 = (DirWithMetadataWrapper) path2;
            assertArrayEquals(wrapped1.metadata(context).join(), Tuple.from(dir1, dir1.length()).pack());
            assertArrayEquals(wrapped2.metadata(context).join(), Tuple.from(dir2, dir2.length()).pack());
        }
    }

    @Test
    public void testMetadataFromLookupByKey() {
        String testRoot = "test-root-" + random.nextInt();
        ResolverCreateHooks hooks = new ResolverCreateHooks(DEFAULT_CHECK, DirWithMetadataWrapper::metadataHook);
        KeySpace root = rootForMetadataTests(testRoot, hooks, getGenerator());
        final FDBDatabase database = dbExtension.getDatabase();
        Tuple tuple;
        String dir = "test-string-" + random.nextInt();
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path1 = root.path(testRoot).add("dir_with_metadata_name", dir);
            tuple = path1.toTuple(context);
            context.ensureActive().set(tuple.pack(), Tuple.from(0).pack());

            DirWithMetadataWrapper wrapped = (DirWithMetadataWrapper) path1;
            assertArrayEquals(wrapped.metadata(context).join(), DirWithMetadataWrapper.metadataHook(dir));
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            DirWithMetadataWrapper fromPath = (DirWithMetadataWrapper) root.resolveFromKey(context, tuple).toPath();
            assertArrayEquals(fromPath.metadata(context).join(), DirWithMetadataWrapper.metadataHook(dir));
        }
    }

    @Test
    public void testSeesMetadataUpdates() {
        String testRoot = "test-root-" + random.nextInt();
        Function<FDBRecordContext, CompletableFuture<LocatableResolver>> generator = getGenerator();
        KeySpace root = rootForMetadataTests(testRoot, generator);
        final FDBDatabase database = dbExtension.getDatabase();
        database.setResolverStateRefreshTimeMillis(100);
        String dir = "test-string-" + random.nextInt();
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path1 = root.path(testRoot).add("dir_with_metadata_name", dir);

            DirWithMetadataWrapper wrapped = (DirWithMetadataWrapper) path1;
            assertThat("there's no metadata", wrapped.metadata(context).join(), is(nullValue()));
        }

        try (FDBRecordContext context = database.openContext()) {
            generator.apply(context)
                    .thenCompose(scope -> scope.updateMetadataAndVersion(dir, Tuple.from("new-metadata").pack()))
                    .join();
        }

        eventually("we see the new metadata for the path", () -> {
            try (FDBRecordContext context = database.openContext()) {
                KeySpacePath path1 = root.path(testRoot).add("dir_with_metadata_name", dir);
                return ((DirWithMetadataWrapper) path1).metadata(context).join();
            }
        }, is(Tuple.from("new-metadata").pack()), 120, 10);
    }

    @Test
    public void testListObeysTimeLimits() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root-" + random.nextInt(Integer.MAX_VALUE))
                        .addSubdirectory(new KeySpaceDirectory("a", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("b", KeyType.LONG)
                                        .addSubdirectory(new KeySpaceDirectory("c", KeyType.LONG)))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 3; j++) {
                    for (int k = 0; k < 5; k++) {
                        tr.set(root.path("root")
                                .add("a", i)
                                .add("b", j)
                                .add("c", k)
                                .toTuple(context).pack(), Tuple.from(i + j).pack());
                    }
                }
            }
            tr.commit().join();
        }

        try (FDBRecordContext context = database.openContext()) {
            // Iteration will inject a 1ms pause in each "a" value we iterate over (there are 10 of them)
            // so we want to make the time limit long enough to make *some* progress, but short enough to
            // to make sure we cannot get them all.
            ScanProperties props = new ScanProperties(ExecuteProperties.newBuilder()
                    .setFailOnScanLimitReached(false)
                    .setTimeLimit(5L)
                    .build());

            // The inner and outer iterator are declared here instead of in-line with the call to flatMapPipelined
            // because IntelliJ was having issues groking the call as a single call.
            Function<byte[], RecordCursor<ResolvedKeySpacePath>> aIterator =
                    outerContinuation ->
                            root.path("root").listSubdirectoryAsync(context, "a", outerContinuation, props)
                                    .map(value -> {
                                        sleep(1L);
                                        return value;
                                    });

            BiFunction<ResolvedKeySpacePath, byte[], RecordCursor<ResolvedKeySpacePath>> bIterator =
                    (aPath, innerContinuation) ->
                            aPath.toPath().add("b", 0).listSubdirectoryAsync(context, "c", innerContinuation, props);

            RecordCursor<ResolvedKeySpacePath> cursor = RecordCursor.flatMapPipelined(
                    aIterator,
                    bIterator,
                    null,
                    10
            );


            long count = cursor.getCount().join();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNext().getNoNextReason());
            // With a 1ms delay we should read no more than 5 "a" values (there are a total of 10)
            // and each "c" value has 4 values. so we shouldn't have been able to read more than 40
            // total values.
            assertTrue(count <= 40, "Read too many values, query should have timed out");
        }
    }

    @Test
    public void testListObeysReturnedRowAndScanLimits() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root-" + random.nextInt(Integer.MAX_VALUE))
                        .addSubdirectory(new KeySpaceDirectory("a", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("b", KeyType.LONG))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 2; j++) {
                    tr.set(root.path("root")
                            .add("a", i)
                            .add("b", j)
                            .toTuple(context).pack(), Tuple.from(i + j).pack());
                }
            }
            tr.commit().join();
        }

        doLimitedScan(database, root, 5, Integer.MAX_VALUE, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);
        doLimitedScan(database, root, Integer.MAX_VALUE, 5, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
    }

    private void doLimitedScan(FDBDatabase database, KeySpace root, int returnedRowLimit, int scannedRecordLimit,
                               RecordCursor.NoNextReason noNextReason) {
        try (FDBRecordContext context = database.openContext()) {
            ScanProperties props = new ScanProperties(ExecuteProperties.newBuilder()
                    .setFailOnScanLimitReached(false)
                    .setReturnedRowLimit(returnedRowLimit)
                    .setScannedRecordsLimit(scannedRecordLimit)
                    .build());
            RecordCursor<ResolvedKeySpacePath> cursor = root.path("root").listSubdirectoryAsync(context, "a", null, props);

            long count = cursor.getCount().join();
            assertEquals(noNextReason, cursor.getNext().getNoNextReason());
            assertEquals(Math.min(returnedRowLimit, scannedRecordLimit), count, "Wrong number of results");
        }
    }

    private static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testListReverse() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root-" + random.nextInt(Integer.MAX_VALUE))
                        .addSubdirectory(new KeySpaceDirectory("a", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("b", KeyType.LONG))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 2; j++) {
                    tr.set(root.path("root")
                            .add("a", i)
                            .add("b", j)
                            .toTuple(context).pack(), Tuple.from(i + j).pack());
                }
            }
            tr.commit().join();
        }

        final List<Tuple> results;
        try (FDBRecordContext context = database.openContext()) {
            ScanProperties props = new ScanProperties(ExecuteProperties.newBuilder().build(), true);
            results = root.path("root")
                    .listSubdirectoryAsync(context, "a", null, props).asList().join().stream()
                    .map(ResolvedKeySpacePath::toTuple)
                    .collect(Collectors.toList());
        }

        assertEquals(5, results.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i, ((Long) results.get(4 - i).getLong(1)).intValue());
        }
    }

    private Function<FDBRecordContext, CompletableFuture<LocatableResolver>> getGenerator() {
        String tmpDirLayer = "tmp-dir-layer-" + random.nextLong();
        KeySpace dirLayerKeySpace = new KeySpace(new KeySpaceDirectory(tmpDirLayer, KeyType.STRING, tmpDirLayer));
        return context ->
                CompletableFuture.completedFuture(new ScopedInterningLayer(context.getDatabase(),
                        dirLayerKeySpace.path(tmpDirLayer).toResolvedPath(context)));
    }

    private KeySpace rootForMetadataTests(String name,
                                          Function<FDBRecordContext, CompletableFuture<LocatableResolver>> generator) {
        return rootForMetadataTests(name, ResolverCreateHooks.getDefault(), generator);
    }

    private KeySpace rootForMetadataTests(String name,
                                          ResolverCreateHooks hooks,
                                          Function<FDBRecordContext, CompletableFuture<LocatableResolver>> generator) {
        final FDBDatabase database = dbExtension.getDatabase();


        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory(name, name)
                        .addSubdirectory(new DirectoryLayerDirectory("dir_with_metadata_name", DirWithMetadataWrapper::new, generator, hooks))
        );

        database.run(context -> root.path(name).deleteAllDataAsync(context));
        return root;
    }

    private static class DirWithMetadataWrapper extends KeySpacePathWrapper {
        DirWithMetadataWrapper(KeySpacePath inner) {
            super(inner);
        }

        static byte[] metadataHook(String schoolName) {
            return Tuple.from(schoolName, schoolName.length()).pack();
        }

        CompletableFuture<byte[]> metadata(@Nonnull FDBRecordContext context) {
            return inner.resolveAsync(context).thenApply(PathValue::getMetadata);
        }
    }

    @Test
    public void testCustomDirectoryResolver() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG, 1L)
                        .addSubdirectory(new ConstantResolvingKeySpaceDirectory("toString", KeyType.STRING, KeySpaceDirectory.ANY_VALUE, v -> "val" + v.toString()))
                        .addSubdirectory(new ConstantResolvingKeySpaceDirectory("toWrongType", KeyType.LONG, KeySpaceDirectory.ANY_VALUE, v -> "val" + v.toString())));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            assertEquals(Tuple.from(1L, "val15"),
                    root.path("root").add("toString", 15).toTuple(context));
            assertThrows(RecordCoreArgumentException.class,
                    () -> root.path("root").add("toWrongType", 21L).toTuple(context));
        }
    }

    @Test
    public void testFromTupleWithConstantValue() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG, 1L)
                    .addSubdirectory(new KeySpaceDirectory("dir1", KeyType.STRING, "a"))
                    .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.STRING, "b"))
                    .addSubdirectory(new KeySpaceDirectory("dir3", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Tuple tuple = Tuple.from(1L, "a");
            assertEquals(tuple, root.resolveFromKey(context, tuple).toTuple());
            tuple = Tuple.from(1L, "b");
            assertEquals(tuple, root.resolveFromKey(context, tuple).toTuple());
            final Tuple badTuple1 = Tuple.from(1L, "c", "d");
            assertThrows(RecordCoreArgumentException.class, () -> root.resolveFromKey(context, badTuple1).toTuple(),
                    "key_tuple", badTuple1,
                    "key_tuple_pos", 1);
        }
    }

    @Test
    public void testPathToString() {
        KeySpace root = new KeySpace("foo",
                new KeySpaceDirectory("root", KeyType.LONG)
                        .addSubdirectory(new KeySpaceDirectory("dir1", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.BYTES)))
                        .addSubdirectory(new KeySpaceDirectory("dir3", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("dir4"))));

        final FDBDatabase database = dbExtension.getDatabase();
        final Long fooValue;
        final Long barValue;
        try (FDBRecordContext context = database.openContext()) {
            List<Long> entries = resolveBatch(context, "_foo", "_bar");
            context.commit();
            fooValue = entries.get(0);
            barValue = entries.get(1);
        }

        assertEquals("/foo/root/dir3/dir4",
                root.getDirectory("root").getSubdirectory("dir3").getSubdirectory("dir4").toPathString());

        try (FDBRecordContext context = database.openContext()) {
            assertEquals("/root:4/dir1:\"hi\"/dir2:0x4142+(\"blah\")",
                    root.resolveFromKey(context, Tuple.from(4L, "hi", new byte[] { 0x41, 0x42 }, "blah")).toString());
            assertEquals("/root:11", root.resolveFromKey(context, Tuple.from(11L)).toString());
            assertEquals("/root:14/dir3:4/dir4:\"_bar\"[" + barValue + "]", root.resolveFromKey(context, Tuple.from(14L, 4L, barValue)).toString());

            assertEquals("/root:11/dir3:17/dir4:" + fooValue,
                    root.path("root", 11L)
                            .add("dir3", 17L)
                            .add("dir4", fooValue).toString());
        }
    }

    @Test
    public void testListDoesNotGoTooDeep() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong())
                    .addSubdirectory(new DirectoryLayerDirectory("b")
                            .addSubdirectory(new KeySpaceDirectory("c", KeyType.STRING)
                                    .addSubdirectory(new KeySpaceDirectory("d", KeyType.BYTES)
                                        .addSubdirectory(new KeySpaceDirectory("e", KeyType.LONG))))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                tr.set(root.path("a")
                        .add("b", "foo_" + i)
                        .add("c", "hi_" + i)
                        .add("d", new byte[] { (byte) i } ).toTuple(context).pack(), Tuple.from(i).pack());
            }
            context.commit();
        }

        // Even though the keyspace understands directories "a"/"b"/"c" we want to make sure that if you
        // list all "b" directories you only get a path leading up to the "b"'s, wth the "c" values as a
        // remainder on the "b" path entry.
        try (FDBRecordContext context = database.openContext()) {
            List<ResolvedKeySpacePath> paths;

            // Check listing from the root
            paths = root.listDirectory(context, "a");
            assertThat("Number of paths in 'a'", paths.size(), is(1));
            assertThat("Value of subdirectory 'a'", paths.get(0).getLogicalValue(), is(root.getDirectory("a").getValue()));
            assertThat("Remainder size of 'a'", paths.get(0).getRemainder().size(), is(3));

            // List from "b"
            paths = root.path("a").listSubdirectory(context, "b");
            assertThat("Number of paths in 'b'", paths.size(), is(5));
            for (ResolvedKeySpacePath path : paths) {
                assertThat("Listing of 'b' directory", path.getDirectoryName(), is("b"));
                Tuple remainder = path.getRemainder();
                assertThat("Remainder of 'b'", remainder.size(), is(2));
                assertThat("Remainder of 'b', first tuple value", remainder.getString(0), startsWith("hi_"));
                assertThat("Remainder of 'b', second tuple value", remainder.getBytes(1), instanceOf(byte[].class));
            }

            // List from "c"
            paths = root.path("a").add("b", "foo_0").listSubdirectory(context, "c");
            assertThat("Number of paths in 'c'", paths.size(), is(1));
            for (ResolvedKeySpacePath path  : paths) {
                assertThat("Listing of 'c' directory", path.getDirectoryName(), is("c"));
                final Tuple remainder = path.getRemainder();
                assertThat("Remainder of 'c'", remainder.size(), is(1));
                assertThat("Remainder of 'c', first tuple value", remainder.getBytes(0), instanceOf(byte[].class));
            }

            // List from "d"
            paths = root.path("a").add("b", "foo_0").add("c", "hi_0").listSubdirectory(context, "d");
            assertThat("Number of paths in 'd'", paths.size(), is(1));
            ResolvedKeySpacePath path = paths.get(0);
            assertThat("Remainder of 'd'", path.getRemainder(), is((Tuple) null));

            // List from "e" (which has no data)
            paths = root.path("a").add("b", "foo_0").add("c", "hi_0").add("d", new byte[] { 0x00 }).listSubdirectory(context, "e");
            assertThat("Number of paths in 'e'", paths.size(), is(0));
        }
    }

    @Test
    public void testDeleteAllDataAndHasData() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG, Math.abs(random.nextLong()))
                        .addSubdirectory(new KeySpaceDirectory("dir1", KeyType.STRING, "a")
                                .addSubdirectory(new KeySpaceDirectory("dir1_1", KeyType.LONG)))
                        .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.STRING, "b")
                                .addSubdirectory(new KeySpaceDirectory("dir2_1", KeyType.LONG)))
                        .addSubdirectory(new KeySpaceDirectory("dir3", KeyType.STRING, "c")
                                .addSubdirectory(new KeySpaceDirectory("dir3_1", KeyType.LONG))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                tr.set(root.path("root").add("dir1").add("dir1_1", i).toTuple(context).pack(), Tuple.from(i).pack());
                tr.set(root.path("root").add("dir2").add("dir2_1", i).toTuple(context).pack(), Tuple.from(i).pack());
                tr.set(root.path("root").add("dir3").add("dir3_1", i).toTuple(context).pack(), Tuple.from(i).pack());
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            // All directories hava data?
            for (int i = 1; i <= 3; i++) {
                assertTrue(root.path("root").add("dir" + i).hasData(context), "dir" + i + " is empty!");
            }
            // Clear out dir2
            root.path("root").add("dir2").deleteAllData(context);
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            assertTrue(root.path("root").add("dir1").hasData(context), "dir1 is empty!");
            assertFalse(root.path("root").add("dir2").hasData(context), "dir2 has data!");
            assertTrue(root.path("root").add("dir3").hasData(context), "dir3 is empty!");
        }
    }

    @Test
    public void testListAnyValue() {
        // Create a root directory called "a" with subdirs of every type (no constants for now)
        Long rootValue = random.nextLong();
        KeySpaceDirectory dirA = new KeySpaceDirectory("a", KeyType.LONG, rootValue);
        for (KeyTypeValue kv : valueOfEveryType) {
            dirA.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType));
        }
        KeySpace root = new KeySpace(dirA);

        final FDBDatabase database = dbExtension.getDatabase();

        final Map<KeyType, List<Tuple>> valuesForType = new HashMap<>();

        // Create an entry in the keyspace with a row for every type that we support
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (KeyTypeValue kv : valueOfEveryType) {
                List<Tuple> values = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    Object value = kv.generator.get();
                    Tuple tupleValue = Tuple.from(value);
                    if (! values.contains(tupleValue)) {
                        values.add(tupleValue);

                        // Make sure that we have extra values in the same keyspace that don't get included in the
                        // final results.
                        for (int j = 0; j < 5; j++) {
                            tr.set(Tuple.from(rootValue, value, j).pack(), Tuple.from(i).pack());
                        }
                    }
                }
                valuesForType.put(kv.keyType, values);
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            for (KeyTypeValue kv : valueOfEveryType) {
                if (kv.keyType != KeyType.NULL) {
                    List<Tuple> values = valuesForType.get(kv.keyType);
                    for (Pair<ValueRange<Object>, List<Tuple>> testCase : listRangeTestCases(values)) {
                        testListRange(testCase.getLeft(), testCase.getRight(), context, root, kv.keyType);
                    }
                }
            }
        }
    }
    
    private Pair<ValueRange<Object>, List<Tuple>> newTestCase(@Nullable Tuple low, @Nullable Tuple high,
                                                              @Nonnull EndpointType lowEndpoint,
                                                              @Nonnull EndpointType highEndpoint,
                                                              List<Tuple> expectedValues) {
        return Pair.of(
                new ValueRange<>(low == null ? null : low.get(0), high == null ? null : high.get(0), 
                        lowEndpoint, highEndpoint),
                expectedValues
        );
    }
    
    private List<Pair<ValueRange<Object>, List<Tuple>>> listRangeTestCases(List<Tuple> values) {
        values.sort(null);

        // Size >= 1. It is very likely to be 5 (but can be smaller) expect when the key type is NULL or BOOLEAN.
        int size = values.size();
        List<Pair<ValueRange<Object>, List<Tuple>>> testCases = new LinkedList<>();
        testCases.add(Pair.of(null,
                new ArrayList<>(values)));
        testCases.add(newTestCase(values.get(0), null, EndpointType.RANGE_INCLUSIVE, EndpointType.TREE_END,
                new ArrayList<>(values)));
        testCases.add(newTestCase(values.get(0), null, EndpointType.RANGE_EXCLUSIVE, EndpointType.TREE_END,
                new ArrayList<>(values.subList(1, size))));
        testCases.add(newTestCase(null, values.get(size - 1), EndpointType.TREE_START, EndpointType.RANGE_INCLUSIVE,
                new ArrayList<>(values)));
        testCases.add(newTestCase(null, values.get(size - 1), EndpointType.TREE_START, EndpointType.RANGE_EXCLUSIVE,
                new ArrayList<>(values.subList(0, size - 1))));
        testCases.add(newTestCase(null, null, EndpointType.TREE_START, EndpointType.TREE_END,
                new ArrayList<>(values)));
        testCases.add(newTestCase(values.get(0), values.get(0), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE,
                new ArrayList<>(values.subList(0, 1))));

        // Only test this for LONG because it might be tricky to modify values for some other types.
        if (KeyType.LONG.isMatch(values.get(0))) {
            Tuple first = values.get(0);
            Tuple justBeforeFirst = Tuple.from((Long)first.get(0) - 1);
            Tuple justAfterFirst = Tuple.from((Long)first.get(0) + 1);

            Tuple last = values.get(size - 1);
            Tuple justBeforeLast = Tuple.from((Long)last.get(0) - 1);
            Tuple justAfterLast = Tuple.from((Long)last.get(0) + 1);

            // Endpoint type does not matters to the values who are not in the collection.
            for (EndpointType endpointType : Arrays.asList(EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE)) {
                testCases.add(newTestCase(justBeforeFirst, null, endpointType, EndpointType.TREE_END,
                        new ArrayList<>(values.subList(0, size))));
                testCases.add(newTestCase(justAfterFirst, null, endpointType, EndpointType.TREE_END,
                        new ArrayList<>(values.subList(1, size))));

                testCases.add(newTestCase(null, justBeforeLast, EndpointType.TREE_START, endpointType,
                        new ArrayList<>(values.subList(0, size - 1))));
                testCases.add(newTestCase(null, justAfterLast, EndpointType.TREE_START, endpointType,
                        new ArrayList<>(values.subList(0, size))));
            }
        }

        if (size >= 2) {
            Tuple first = values.get(0);
            Tuple last = values.get(size - 1);
            testCases.add(newTestCase(first, last, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE,
                    new ArrayList<>(values.subList(0, size))));
            testCases.add(newTestCase(first, last, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE,
                    new ArrayList<>(values.subList(0, size - 1))));
            testCases.add(newTestCase(first, last, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_INCLUSIVE,
                    new ArrayList<>(values.subList(1, size))));
            testCases.add(newTestCase(first, last, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_EXCLUSIVE,
                    new ArrayList<>(values.subList(1, size - 1))));
        }

        if (size >= 4) {
            Tuple second = values.get(1);
            Tuple secondLast = values.get(size - 2);
            testCases.add(newTestCase(second, secondLast, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE,
                    new ArrayList<>(values.subList(1, size - 1))));
            testCases.add(newTestCase(second, secondLast, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE,
                    new ArrayList<>(values.subList(1, size - 2))));
            testCases.add(newTestCase(second, secondLast, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_INCLUSIVE,
                    new ArrayList<>(values.subList(2, size - 1))));
            testCases.add(newTestCase(second, secondLast, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_EXCLUSIVE,
                    new ArrayList<>(values.subList(2, size - 2))));
        }
        return testCases;
    }

    private void testListRange(ValueRange<Object> range,
                               List<Tuple> expectedValues,
                               FDBRecordContext context,
                               KeySpace root,
                               KeyType keyType) {
        String testCaseInfo = KeyValueLogMessage.build(" at testListRange",
                "range", range,
                "expectedValues", expectedValues,
                "keyType", keyType).toString();

        List<ResolvedKeySpacePath> paths = context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST,
                root.path("a")
                        .listSubdirectoryAsync(context, keyType.toString(), range, null, ScanProperties.FORWARD_SCAN).asList());

        assertEquals(expectedValues.size(), paths.size(), "The result size does not match" + testCaseInfo);

        for (ResolvedKeySpacePath path : paths) {
            Tuple tuple = path.toTuple();
            assertTrue(expectedValues.remove(Tuple.from(tuple.get(1))), "missing: " + tuple.get(1) + testCaseInfo);
        }

        assertTrue(expectedValues.isEmpty(), "Missing values: " + expectedValues + testCaseInfo);
    }

    @Test
    public void testInvalidListRange() throws Exception {
        final String rootDir = "root_dir";
        final String stringDir = "string_dir";
        final String longConstDir = "long_const_dir";
        KeySpaceDirectory dirA = new KeySpaceDirectory(rootDir, KeyType.LONG, random.nextLong())
                .addSubdirectory(new KeySpaceDirectory(stringDir, KeyType.STRING))
                .addSubdirectory(new KeySpaceDirectory(longConstDir, KeyType.LONG, 100));
        KeySpace root = new KeySpace(dirA);

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            // Positive example.
            root.path(rootDir)
                    .listSubdirectory(context, stringDir,
                            new ValueRange<>("A", "B", EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE),
                            null,
                            ScanProperties.FORWARD_SCAN);

            // The range value should be in the same type.
            assertThrows(RecordCoreArgumentException.class, () ->
                    root.path(rootDir).listSubdirectory(
                            context,
                            stringDir,
                            new ValueRange<>(100, 200, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE),
                            null,
                            ScanProperties.FORWARD_SCAN)
            );

            // PREFIX_STRING should not be used as a endpoint type.
            assertThrows(RecordCoreArgumentException.class, () ->
                    root.path(rootDir).listSubdirectory(
                            context,
                            stringDir,
                            new ValueRange<>("A", "B", EndpointType.PREFIX_STRING, EndpointType.RANGE_EXCLUSIVE),
                            null,
                            ScanProperties.FORWARD_SCAN)
            );

            // Range should be null when the subdirectory has a value.
            assertThrows(RecordCoreArgumentException.class, () ->
                    root.path(rootDir).listSubdirectory(
                            context,
                            longConstDir,
                            new ValueRange<>(100, 200, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE),
                            null,
                            ScanProperties.FORWARD_SCAN)
            );
        }
    }

    @Test
    public void testListAcrossTransactions() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong())
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING)));

        final FDBDatabase database = dbExtension.getDatabase();
        final List<String> directoryEntries = IntStream.range(0, 10).boxed().map(i -> "val_" + i).collect(Collectors.toList());

        final KeySpacePath rootPath = root.path("a");
        try (final FDBRecordContext context = database.openContext()) {
            final Transaction tr = context.ensureActive();
            directoryEntries.forEach(name -> tr.set(rootPath.add("b",  name).toTuple(context).pack(), TupleHelpers.EMPTY.pack()));
            context.commit();
        }

        byte[] continuation = null;
        int idx = 0;
        do {
            try (final FDBRecordContext context = database.openContext()) {
                final RecordCursor<ResolvedKeySpacePath>  cursor = rootPath.listSubdirectoryAsync(context, "b", continuation,
                        new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()));
                List<ResolvedKeySpacePath> subdirs = context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST, cursor.asList());
                if (!subdirs.isEmpty()) {
                    assertEquals(2, subdirs.size(), "Wrong number of path entries returned");
                    assertEquals("val_" + idx, subdirs.get(0).getResolvedValue());
                    assertEquals("val_" + (idx + 1), subdirs.get(1).getResolvedValue());
                    idx += 2;
                    continuation = cursor.getNext().getContinuation().toBytes();
                    System.out.println(continuation == null ? "null" : Tuple.fromBytes(continuation));
                } else {
                    continuation = cursor.getNext().getContinuation().toBytes();
                    assertNull(continuation);
                }
            }
        } while (continuation != null);

        assertEquals(directoryEntries.size(), idx);
    }

    private static class TestWrapper1 extends KeySpacePathWrapper {
        public TestWrapper1(KeySpacePath inner) {
            super(inner);
        }
    }

    @Test
    public void testListConstantValue() {
        // Create a root directory called "a" with subdirs of every type and a constant value
        Long rootValue = random.nextLong();
        KeySpaceDirectory dirA = new KeySpaceDirectory("a", KeyType.LONG, rootValue);
        for (KeyTypeValue kv : valueOfEveryType) {
            dirA.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType, kv.generator.get()));
        }
        KeySpace root = new KeySpace(dirA);

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (KeyTypeValue kv : valueOfEveryType) {
                KeySpaceDirectory dir = root.getDirectory("a").getSubdirectory(kv.keyType.name());
                for (int i = 0; i < 5; i++) {
                    tr.set(Tuple.from(rootValue, dir.getValue(), i).pack(), Tuple.from(i).pack());
                }
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            for (KeyTypeValue kv : valueOfEveryType) {
                KeySpaceDirectory dir = root.getDirectory("a").getSubdirectory(kv.keyType.name());

                List<ResolvedKeySpacePath> paths = root.path("a").listSubdirectory(context, kv.keyType.toString());
                assertEquals(1, paths.size());
                if (dir.getKeyType() == KeyType.BYTES) {
                    assertArrayEquals((byte[])dir.getValue(), paths.get(0).toTuple().getBytes(1));
                } else {
                    assertEquals(dir.getValue(), paths.get(0).toTuple().get(1));
                }
            }
        }
    }

    @Test
    public void testListDirectoryLayer() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong())
                        .addSubdirectory(new DirectoryLayerDirectory("b"))
                        .addSubdirectory(new KeySpaceDirectory("c", KeyType.STRING, "c")
                                .addSubdirectory(new DirectoryLayerDirectory("d", "d"))
                                .addSubdirectory(new DirectoryLayerDirectory("e", "e"))
                                .addSubdirectory(new DirectoryLayerDirectory("f", "f"))));

        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 5; j++) {
                    Tuple key = root.path("a").add("b", "value_" + i).toTuple(context).add(i).add(j);
                    tr.set(key.pack(), Tuple.from(i).pack());
                }
            }

            for (int i = 0; i < 5; i++) {
                tr.set(root.path("a").add("c").add("d").toTuple(context).add(i).pack(), Tuple.from(i).pack());
                tr.set(root.path("a").add("c").add("e").toTuple(context).add(i).pack(), Tuple.from(i).pack());
                tr.set(root.path("a").add("c").add("f").toTuple(context).add(i).pack(), Tuple.from(i).pack());
            }

            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            List<ResolvedKeySpacePath> paths = root.path("a").listSubdirectory(context, "b");
            assertEquals(10, paths.size());

            for (ResolvedKeySpacePath path : paths) {
                final long index = path.getRemainder().getLong(0); // The first part of the remainder was the index
                // We should always get the "first" key for the value in the directory
                assertEquals(0, path.getRemainder().getLong(1));
                assertTrue(index >= 0 && index < 10);
                assertEquals("a", path.getParent().getDirectoryName());
                assertEquals(root.getDirectory("a").getValue(), path.getParent().getLogicalValue());
                assertEquals("value_" + index, path.getLogicalValue());
            }

            for (String subdir : ImmutableList.of("d", "e", "f")) {
                paths = root.path("a").add("c").listSubdirectory(context, subdir);
                assertEquals(1, paths.size());
                assertEquals(subdir, paths.get(0).getLogicalValue());
                assertEquals(0L, paths.get(0).getRemainder().getLong(0));
                assertEquals("c", paths.get(0).getParent().getDirectoryName());
                assertEquals("a", paths.get(0).getParent().getParent().getDirectoryName());
            }
        }
    }

    /*
     * This isn't specifically just a test, but is also here to demonstrate how you can use the KeySpacePath
     * wrapping facility to work with paths in a type safe manner.
     */
    @Test
    public void testPathWrapperExample() throws Exception {
        EnvironmentKeySpace keySpace = new EnvironmentKeySpace("production");
        final FDBDatabase database = dbExtension.getDatabase();

        // Create a tuple to represent a path to a user's main store. This will trigger the creation of the
        // necessary directory layer entries.
        final Tuple dataStoreTuple;
        final Tuple metadataStoreTuple;
        try (FDBRecordContext context = database.openContext()) {
            ApplicationPath application = keySpace.root().userid(123).application("myApplication");
            dataStoreTuple = application.dataStore().toTuple(context);
            metadataStoreTuple = application.metadataStore().toTuple(context);
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            List<Long> entries = resolveBatch(context, keySpace.getRootName(), "myApplication");

            // Validate the entries created above look like what we expect
            assertEquals(Tuple.from(entries.get(0), 123L, entries.get(1), EnvironmentKeySpace.DATA_VALUE), dataStoreTuple);
            assertEquals(Tuple.from(entries.get(0), 123L, entries.get(1), EnvironmentKeySpace.METADATA_VALUE), metadataStoreTuple);

            ResolvedKeySpacePath path =  keySpace.fromKey(context, dataStoreTuple);
            assertThat(path.toPath(), instanceOf(DataPath.class));

            DataPath mainStorePath = (DataPath) path.toPath();
            assertEquals(EnvironmentKeySpace.DATA_VALUE, mainStorePath.getValue());
            assertEquals(EnvironmentKeySpace.DATA_VALUE, mainStorePath.resolveAsync(context).get().getResolvedValue());
            assertEquals(entries.get(1), mainStorePath.parent().resolveAsync(context).get().getResolvedValue());
            assertEquals("myApplication", mainStorePath.parent().getValue());
            assertEquals(123L, mainStorePath.parent().parent().getValue());
            assertEquals(entries.get(0), mainStorePath.parent().parent().parent().resolveAsync(context).get().getResolvedValue());
            assertEquals("production", mainStorePath.parent().parent().parent().getValue());
            assertNull(mainStorePath.parent().parent().parent().parent());

            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 1)).toPath(), instanceOf(EnvironmentRoot.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 2)).toPath(), instanceOf(UserPath.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 3)).toPath(), instanceOf(ApplicationPath.class));

            path = keySpace.fromKey(context, metadataStoreTuple);
            assertThat(path.toPath(), instanceOf(MetadataPath.class));

            MetadataPath metadataPath = (MetadataPath) path.toPath();
            assertEquals(EnvironmentKeySpace.METADATA_VALUE, metadataPath.getValue());
            assertEquals(EnvironmentKeySpace.METADATA_VALUE, metadataPath.resolveAsync(context).get().getResolvedValue());
            assertEquals(entries.get(1), metadataPath.parent().resolveAsync(context).get().getResolvedValue());
            assertEquals("myApplication", metadataPath.parent().getValue());
            assertEquals(123L, metadataPath.parent().parent().getValue());
            assertEquals(entries.get(0), metadataPath.parent().parent().parent().resolveAsync(context).get().getResolvedValue());
            assertEquals("production", metadataPath.parent().parent().parent().getValue());
            assertNull(metadataPath.parent().parent().parent().parent());

            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 1)).toPath(), instanceOf(EnvironmentRoot.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 2)).toPath(), instanceOf(UserPath.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 3)).toPath(), instanceOf(ApplicationPath.class));

            // Create a fake main store "record" key to demonstrate that we can get the key as the remainder
            Tuple recordTuple = dataStoreTuple.add(1L).add("someStr").add(0L); // 1=record space, record id, 0=unsplit record
            path =  keySpace.fromKey(context, recordTuple);
            assertThat(path.toPath(), instanceOf(DataPath.class));
            assertEquals(Tuple.from(1L, "someStr", 0L), path.getRemainder());
            assertEquals(dataStoreTuple, path.toTuple());
        }
    }

    // This isn't so much a test as validation to ensure that the code and output that is used in the comments
    // in some of the implementing classes works as advertised.
    @Test
    public void testToTree() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("state", KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("office_id", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("employees", KeyType.STRING, "E")
                                        .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG)))
                                .addSubdirectory(new KeySpaceDirectory("inventory", KeyType.STRING, "I")
                                        .addSubdirectory(new KeySpaceDirectory("stock_id", KeyType.LONG)))
                                .addSubdirectory(new KeySpaceDirectory("sales", KeyType.STRING, "S")
                                        .addSubdirectory(new KeySpaceDirectory("transaction_id", KeyType.UUID))
                                        .addSubdirectory(new KeySpaceDirectory("layaways", KeyType.NULL)
                                                .addSubdirectory(new KeySpaceDirectory("transaction_id", KeyType.UUID))))));
        System.out.println(keySpace);
    }

    @Test
    public void testAddToPathPreservesParentWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                    .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new))
                    .addSubdirectory(new DirectoryLayerDirectory("c", PathC::new)));
        PathA a = (PathA) keySpace.path("a", "foo");
        PathB b = (PathB) a.add("b", "bar");
        PathC c = (PathC) a.add("c", "bax");
        assertThat("parent of b should be a PathA", b.getParent(), instanceOf(PathA.class));
        assertThat("parent of c should be a PathA", c.getParent(), instanceOf(PathA.class));
    }

    @Test
    public void testListPreservesWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new)));
        final FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            PathA root = (PathA) keySpace.path("a", "foo");
            tr.set(root.add("b", "one").toTuple(context).pack(), TupleHelpers.EMPTY.pack());
            tr.set(root.add("b", "two").toTuple(context).pack(), TupleHelpers.EMPTY.pack());
            tr.set(root.add("b", "three").toTuple(context).pack(), TupleHelpers.EMPTY.pack());
            tr.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            List<ResolvedKeySpacePath> paths = keySpace.path("a", "foo").listSubdirectory(context, "b");
            for (ResolvedKeySpacePath path : paths) {
                assertThat("Path should be PathB", path, instanceOf(PathB.class));
                assertThat("parent should be PathA", path.getParent(), instanceOf(PathA.class));
            }
        }
    }

    @Test
    public void flattenPreservesWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new)));
        List<KeySpacePath> path = keySpace.path("a", "foo").add("b", "bar").flatten();
        assertThat("a should be pathA", path.get(0), instanceOf(PathA.class));
        assertThat("b should be pathB", path.get(1), instanceOf(PathB.class));
    }

    @Test
    public void testPathCompareByValue() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("d", KeyType.STRING, TestWrapper1::new)))
                        .addSubdirectory(new KeySpaceDirectory("c", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("d", KeyType.STRING, TestWrapper1::new))));

        final KeySpacePath p1 = keySpace.path("a", "alpha")
                .add("b", "bravo")
                .add("d", "delta");

        final KeySpacePath p2 = keySpace.path("a", "alpha")
                .add("b", "bravo")
                .add("d", "duck");

        final KeySpacePath p3 = keySpace.path("a", "alpha")
                .add("c", 23)
                .add("d", "delta");

        final KeySpacePath sameAsP1 = keySpace.path("a", "alpha")
                .add("b", "bravo")
                .add("d", "delta");

        assertNotEquals(p1, p2, "paths have different values");
        assertNotEquals(p1.hashCode(), p2.hashCode(), "they have distinct hash codes");
        assertNotEquals(p1, p3, "paths have different parents");
        assertNotEquals(p1.hashCode(), p3.hashCode(), "they have distinct hash codes");

        assertNotSame(p1, sameAsP1, "the paths are not equal by reference");
        assertEquals(p1, sameAsP1, "they are equal by value");
        assertEquals(p1.hashCode(), sameAsP1.hashCode(), "they have the same hash code");
    }

    /**
     * {@code KeySpaceDirectory}s are supposed to be inserted into a singleton {@link KeySpace}, thus we can use
     * reference equality to do comparisons. This is particularly important for the efficiency of
     * {@link KeySpacePathImpl#equals(Object)}, because we don't want it to have to re-compare all of the children of the
     * directory as you go up through the parents. If using reference equality turns out to be problematic,
     * we'll want to look at other solutions, such as ignoring the hierarchy, or something more tricky.
     */
    @Test
    void testKeySpaceDirectoryEqualsUsesReferenceEquality() {
        // Create two directories with identical properties
        KeySpaceDirectory dir1 = new KeySpaceDirectory("test", KeyType.STRING, "value");
        KeySpaceDirectory dir2 = new KeySpaceDirectory("test", KeyType.STRING, "value");

        // KeySpaceDirectory.equals should use reference equality
        assertEquals(dir1, dir1, "Directory should equal itself");
        assertNotEquals(dir1, dir2, "Directories with same properties should not be equal (reference equality)");

        // Test with different properties
        KeySpaceDirectory dir3 = new KeySpaceDirectory("different", KeyType.LONG, 42L);
        assertNotEquals(dir1, dir3, "Directories with different properties should not be equal");

        // Test with null
        assertNotEquals(dir1, null, "Directory should not equal null, and calling with null shouldn't error");

        // Test with different object type
        assertNotEquals(dir1, "not a directory", "Directory should not equal a different type");
    }

    @Test
    void testKeySpaceDirectoryHashCodeFollowsReferenceSemantics() {
        // Create two directories with identical properties
        KeySpaceDirectory dir1 = new KeySpaceDirectory("test", KeyType.STRING, "value");
        KeySpaceDirectory dir2 = new KeySpaceDirectory("test", KeyType.STRING, "value");

        // Since equals uses reference equality, hashCode should be consistent with that
        // (i.e., objects that are equal should have the same hashCode, but since these
        // objects are not equal by reference, their hashCodes may differ)

        // The same object should always have the same hashCode
        int hashCode1 = dir1.hashCode();
        assertEquals(hashCode1, dir1.hashCode(), "Same object should produce same hashCode");

        // Different instances (even with same properties) may have different hashCodes
        // We can't assert they're different, but we can verify the hashCode is stable
        int hashCode2 = dir2.hashCode();
        assertEquals(hashCode2, dir2.hashCode(), "Same object should produce same hashCode");

        // Test that hashCode is consistent across multiple calls
        for (int i = 0; i < 10; i++) {
            assertEquals(hashCode1, dir1.hashCode(), "hashCode should be stable across calls");
            assertEquals(hashCode2, dir2.hashCode(), "hashCode should be stable across calls");
        }
        // two difference references may have the same hash code, but eventually we should find a different one, even
        // though all properties are the same
        for (int i = 0; i < 100; i++) {
            assertNotEquals(hashCode1, new KeySpaceDirectory("test", KeyType.STRING, "value").hashCode());
        }
    }

    private List<Long> resolveBatch(FDBRecordContext context, String... names) {
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (String name : names) {
            futures.add(ScopedDirectoryLayer.global(context.getDatabase()).resolve(context.getTimer(), name));
        }
        return AsyncUtil.getAll(futures).join();
    }

    /** Used to validate wrapping of path names. */
    public static class PathA extends KeySpacePathWrapper {
        public PathA(KeySpacePath parent) {
            super(parent);
        }
    }

    /** Used to validate wrapping of path names. */
    public static class PathB extends KeySpacePathWrapper {
        public PathB(KeySpacePath parent) {
            super(parent);
        }
    }

    /** Used to validate wrapping of path names. */
    public static class PathC extends KeySpacePathWrapper {
        public PathC(KeySpacePath parent) {
            super(parent);
        }
    }

    private static class ConstantResolvingKeySpaceDirectory extends KeySpaceDirectory {

        private final Function<Object, Object> resolver;

        public ConstantResolvingKeySpaceDirectory(String name, KeyType keyType, Object constantValue, Function<Object, Object> resolver) {
            super(name, keyType, constantValue);
            this.resolver = resolver;
        }

        @Nonnull
        @Override
        protected CompletableFuture<PathValue> toTupleValueAsyncImpl(@Nonnull FDBRecordContext context, Object value) {
            return CompletableFuture.completedFuture(new PathValue(resolver.apply(value)));
        }
    }

    /**
     * This provides an example of a way in which you can define a KeySpace in a relatively clean and type-safe
     * manner. It defines a keyspace that looks like:
     * <pre>
     *    [environment]           - A string the identifies the logical environment (like prod, test, qa, etc.).
     *      |                       This string is converted by the directory layer as a small integer value.
     *      +- userid             - An integer ID for each user in the system
     *         |
     *         +- [application]   - Tne name of an application the user runs (again, converted by the directory
     *            |                 layer into a small integer value)
     *            +- data=1       - Constant value of "1", which is the location of a {@link FDBRecordStore}
     *            |                 in which application data is to be stored
     *            +- metadata=2   - Constant value of "2", which is the Location of another <code>FDBRecordStore</code>
     *                              in which application metadata or configuration information can live.
     * </pre>
     * The main point of this class is to demonstrate how you can use the KeySpacePath wrapping facility to provide
     * implementations of the path elements that are meaningful to your application environment and type safe.
     */
    private static class EnvironmentKeySpace {
        private final KeySpace root;
        private final String rootName;

        public static String USER_KEY = "userid";
        public static String APPLICATION_KEY = "application";
        public static String DATA_KEY = "data";
        public static long DATA_VALUE = 1L;
        public static String METADATA_KEY = "metadata";
        public static long METADATA_VALUE = 2L;

        /**
         * The <code>EnvironmentKeySpace</code> scopes all of the data it stores underneath of a <code>rootName</code>,
         * for example, you could define an instance for <code>prod</code>, <code>test</code>, <code>qa</code>, etc.
         *
         * @param rootName The root name underwhich all data is stored.
         */
        public EnvironmentKeySpace(String rootName) {
            this.rootName = rootName;
            root = new KeySpace(
                    new DirectoryLayerDirectory(rootName, rootName, EnvironmentRoot::new)
                            .addSubdirectory(new KeySpaceDirectory(USER_KEY, KeyType.LONG, UserPath::new)
                                    .addSubdirectory(new DirectoryLayerDirectory(APPLICATION_KEY, ApplicationPath::new)
                                            .addSubdirectory(new KeySpaceDirectory(DATA_KEY, KeyType.LONG, DATA_VALUE, DataPath::new))
                                            .addSubdirectory(new KeySpaceDirectory(METADATA_KEY, KeyType.LONG, METADATA_VALUE, MetadataPath::new)))));
        }

        public String getRootName() {
            return rootName;
        }

        /**
         * Returns an implementation of a <code>KeySpacePath</code> that represents the start of the environment.
         */
        public EnvironmentRoot root()  {
            return (EnvironmentRoot) root.path(rootName);
        }

        /**
         * Given a tuple that represents an FDB key that came from this KeySpace, returns the leaf-most path
         * element in which the tuple resides.
         */
        public ResolvedKeySpacePath fromKey(FDBRecordContext context, Tuple tuple) {
            return root.resolveFromKey(context, tuple);
        }
    }

    /**
     * A <code>KeySpacePath</code> that represents the logical root of the environment.
     */
    private static class EnvironmentRoot extends KeySpacePathWrapper {
        public EnvironmentRoot(KeySpacePath path) {
            super(path);
        }

        public KeySpacePath parent() {
            return null;
        }

        public UserPath userid(long userid) {
            return (UserPath) inner.add(EnvironmentKeySpace.USER_KEY, userid);
        }
    }

    private static class UserPath extends KeySpacePathWrapper {
        public UserPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath application(String applicationName) {
            return (ApplicationPath) inner.add(EnvironmentKeySpace.APPLICATION_KEY, applicationName);
        }

        public EnvironmentRoot parent() {
            return (EnvironmentRoot) inner.getParent();
        }
    }

    private static class ApplicationPath extends KeySpacePathWrapper {
        public ApplicationPath(KeySpacePath path) {
            super(path);
        }

        public DataPath dataStore() {
            return (DataPath) inner.add(EnvironmentKeySpace.DATA_KEY);
        }

        public MetadataPath metadataStore() {
            return (MetadataPath) inner.add(EnvironmentKeySpace.METADATA_KEY);
        }

        public UserPath parent() {
            return (UserPath) inner.getParent();
        }
    }

    private static class DataPath extends KeySpacePathWrapper {
        public DataPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath parent() {
            return (ApplicationPath) inner.getParent();
        }
    }

    private static class MetadataPath extends KeySpacePathWrapper {
        public MetadataPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath parent() {
            return (ApplicationPath) inner.getParent();
        }
    }
}
