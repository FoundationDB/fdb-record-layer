/*
 * TestFDBDirectory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.hamcrest.Matchers;
import org.junit.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Wrapper for {@link FDBDirectory} to better support tests provided by lucene.
 */
public class TestFDBDirectory extends FDBDirectory {
    private static final LuceneOptimizedFieldInfosFormat FIELD_INFOS_FORMAT = new LuceneOptimizedFieldInfosFormat();
    private static boolean fullBufferToSurviveDeletes;
    private static boolean allowAddIndexes;
    private static boolean calledAddIndexes;

    private static final AtomicReference<NonnullPair<String, FieldInfos>> previousFieldInfos = new AtomicReference<>();
    private static final AtomicReference<NonnullPair<String, Map<Long, byte[]>>> previousStoredFields = new AtomicReference<>();
    /**
     * Whether to block any calls to {@code addIndexes}. This is useful if we need tests are failing in a way that could
     * indicate that {@code addIndexes} is being called.
     */
    private static boolean blockAddIndexes = true;
    private static boolean disableFieldInfosCountCheck;

    public TestFDBDirectory() {
        super(new Subspace(Tuple.from("record-test", "unit", "lucene")),
                BaseIndexFileFormatTestCaseUtils.dbExtension.getDatabase().openContext(),
                Map.of(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));
    }

    /**
     * Buffer all {@link IndexInput} content on {@code #openInput} so that it can continue to be read after the
     * associated file has been deleted.
     * <p>
     * Some tests depend on the following behavior, that is hopefully only a test behavior, and not a real Lucene
     * dependency:
     *     <ol>
     *         <li>open an {@code IndexInput}</li>
     *         <li>delete the associated file</li>
     *         <li>read from the {@code IndexInput}</li>
     *     </ol>
     *     This works with {@link org.apache.lucene.store.RAMDirectory} because the {@code IndexInput} that returns just
     *     contains the whole file in a byte array. It works for the various {@link org.apache.lucene.store.FSDirectory}s
     *     because, at least on a mac, if you open a file handle, and then delete the associated file, you can continue
     *     to read from the file (at least as much as is cached).
     * </p>
     * <p>
     *     Right now it only fails for one test, and that test seems suspect, and looks as though it is just a weird
     *     assumption of the test. Anytime we add a new usage of this we should make sure that it's not a sign that
     *     Lucene itself has an assumption that you can read files after they are deleted, and thus we could break
     *     lucene.
     * </p>
     * <p>
     *     If you set this to {@code true} make sure to set it back to {@code false} in a {@code finally} block.
     * </p>
     */
    public static void useFullBufferToSurviveDeletes() {
        TestFDBDirectory.fullBufferToSurviveDeletes = true;
    }

    /**
     * Enables support so that {@link IndexWriter#addIndexes} works correctly.
     * <p>
     * {@link IndexWriter#addIndexes} is used to copy segments from one directory to another. Under the hood this
     * uses {@link org.apache.lucene.store.Directory#copyFrom} to copy the data, which we could override, but
     * {@link org.apache.lucene.store.MockDirectoryWrapper} explicitly does not call into the wrapped function, so
     * there is no clean way for us to copy non-file data, instead it will always just copy the files. This looks
     * to be intentional (<a href="https://issues.apache.org/jira/browse/LUCENE-6299">LUCENE-6299</a> /
     * <a
     * href="https://github.com/apache/lucene/commit/4e53fae38e993fad0c8a5466e1e0c6121d49ad5f#diff-6c29e90918d4bab6eab63cb94f9c0ae55fb14bbbb07446ee0568bb33343947f7">
     * commit</a>.
     * The code that is run when this is enabled analyzes the stack trace and copies the data that is not stored in
     * files to the destination.
     * </p>
     */
    public static void allowAddIndexes() {
        TestFDBDirectory.allowAddIndexes = true;
    }

    public static void reset() {
        fullBufferToSurviveDeletes = false;
        allowAddIndexes = false;
        calledAddIndexes = false;
        disableFieldInfosCountCheck = false;
        previousFieldInfos.set(null);
        previousStoredFields.set(null);
    }

    public static void disableFieldInfosCountCheck() {
        disableFieldInfosCountCheck = true;
    }

    @Nonnull
    @Override
    public IndexInput openInput(@Nonnull final String name, @Nonnull final IOContext ioContext) throws IOException {
        final IndexInput indexInput = super.openInput(name, ioContext);
        if (fullBufferToSurviveDeletes) {
            assertThat("Avoid buffering more than 10MB",
                    indexInput.length(), Matchers.lessThan(10_000_000L));
            final byte[] bytes = new byte[(int)indexInput.length()];
            indexInput.readBytes(bytes, 0, (int)indexInput.length());
            indexInput.close();
            return new ByteBuffersIndexInput(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(bytes))), name);
        }
        if (allowAddIndexes) {
            if (isStacktraceCopySegmentAsIs()) {
                if (name.endsWith("." + LuceneOptimizedFieldInfosFormat.EXTENSION) ||
                        name.endsWith("." + LuceneOptimizedCompoundFormat.ENTRIES_EXTENSION)) {
                    final FieldInfos fieldInfos = FIELD_INFOS_FORMAT.read(this, name);
                    // the test code calls `openInput` twice for the same file before trying to write it, but fieldInfos
                    // does not have a good `.equals()`, so we can't compare just the values
                    final NonnullPair<String, FieldInfos> previous = previousFieldInfos.getAndSet(NonnullPair.of(name, fieldInfos));
                    if (previous != null) {
                        Assert.assertEquals(previous.getLeft(), name);
                    }
                }
                if (name.endsWith("." + LuceneOptimizedCompoundFormat.DATA_EXTENSION) ||
                        name.endsWith("." + LuceneOptimizedStoredFieldsFormat.STORED_FIELDS_EXTENSION)) {
                    final String segmentName = IndexFileNames.parseSegmentName(name);
                    final byte[] key = storedFieldsSubspace.pack(Tuple.from(segmentName));
                    final List<KeyValue> rawStoredFields = asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS,
                            getAgilityContext().instrument(LuceneEvents.Events.LUCENE_READ_STORED_FIELDS,
                                    getAgilityContext().getRange(key, ByteArrayUtil.strinc(key))));
                    final Map<Long, byte[]> storedFields = rawStoredFields.stream().collect(Collectors.toMap(
                            keyValue -> storedFieldsSubspace.unpack(keyValue.getKey()).getLong(1),
                            keyValue -> Objects.requireNonNull(Objects.requireNonNull(getSerializer()).decodeFieldProtobuf(keyValue.getValue()))
                    ));
                    final NonnullPair<String, Map<Long, byte[]>> previous = previousStoredFields.getAndSet(NonnullPair.of(name, storedFields));
                    if (previous != null) {
                        Assert.assertEquals(previous.getLeft(), name);
                    }
                }
            }
        } else if (blockAddIndexes) {
            Assert.assertFalse("Tried to add indexes", isStacktraceCopySegmentAsIs());
        }
        return indexInput;
    }

    @Nonnull
    @Override
    public IndexOutput createOutput(@Nonnull final String name, @Nullable final IOContext ioContext) throws IOException {
        final IndexOutput indexOutput = super.createOutput(name, ioContext);
        if (allowAddIndexes) {
            if (isStacktraceCopySegmentAsIs()) {
                if (name.endsWith("." + LuceneOptimizedFieldInfosFormat.EXTENSION) ||
                        name.endsWith("." + LuceneOptimizedCompoundFormat.ENTRIES_EXTENSION)) {
                    return new WrappedIndexOutput(name, name, indexOutput) {
                        @Override
                        public void close() throws IOException {
                            super.close();
                            final NonnullPair<String, FieldInfos> previous = previousFieldInfos.get();
                            FIELD_INFOS_FORMAT.write(TestFDBDirectory.this, previous.getRight(), name);

                            previousFieldInfos.compareAndSet(previous, null);
                        }
                    };
                }

                if (name.endsWith("." + LuceneOptimizedCompoundFormat.DATA_EXTENSION) ||
                        name.endsWith("." + LuceneOptimizedStoredFieldsFormat.STORED_FIELDS_EXTENSION)) {
                    final NonnullPair<String, Map<Long, byte[]>> previous = previousStoredFields.get();
                    final String segmentName = IndexFileNames.parseSegmentName(name);
                    for (final Map.Entry<Long, byte[]> storedFields : previous.getRight().entrySet()) {
                        writeStoredFields(segmentName, storedFields.getKey().intValue(), storedFields.getValue());
                    }
                    previousStoredFields.compareAndSet(previous, null);
                    return indexOutput;
                }
                calledAddIndexes = true;
            }
        }
        return indexOutput;
    }

    @Override
    public void close() throws IOException {
        if (!calledAddIndexes && !disableFieldInfosCountCheck) {
            assertThat(asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_READ_FIELD_INFOS, getFieldInfosCount()),
                    Matchers.lessThanOrEqualTo(1));
        }
        super.close();
    }

    private static boolean isStacktraceCopySegmentAsIs() {
        return Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(element ->
                element.getClassName().equals(IndexWriter.class.getName()) &&
                        element.getMethodName().equals("addIndexes")) &&
                Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(element ->
                        element.getClassName().equals(IndexWriter.class.getName()) &&
                                element.getMethodName().equals("copySegmentAsIs"));
    }

    private class WrappedIndexOutput extends IndexOutput {
        private final IndexOutput inner;

        /**
         * Sole constructor.  resourceDescription should be non-null, opaque string
         * describing this resource; it's returned from {@link #toString}.
         */
        protected WrappedIndexOutput(final String resourceDescription, final String name, IndexOutput inner) {
            super(resourceDescription, name);
            this.inner = inner;
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }

        @Override
        public long getFilePointer() {
            return inner.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return inner.getChecksum();
        }

        @Override
        public void writeByte(final byte b) throws IOException {
            inner.writeByte(b);
        }

        @Override
        public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
            inner.writeBytes(b, offset, length);
        }
    }
}
