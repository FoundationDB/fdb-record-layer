package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class TestFDBDirectory extends FDBDirectory {
    private static boolean fullBufferToSurviveDeletes;
    private static boolean allowAddIndexes;
    private static byte[] fieldInfosToCopy;

    public TestFDBDirectory() {
        super(new Subspace(Tuple.from("record-test", "unit", "lucene")),
                FDBDatabaseFactory.instance().getDatabase().openContext());
    }

    /**
     * Buffer all {@link IndexInput} content on {@code #openInput} so that it can continue to be read after the
     * associated file has been deleted.
     * <p>
     *     Some tests depend on the following behavior, that is hopefully only a test behavior, and not a real Lucene
     *     dependency:
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
     * @param newValue {@code true} to buffer the contents of all returned {@code IndexInput}s.
     */
    public static void setFullBufferToSurviveDeletes(final boolean newValue) {
        TestFDBDirectory.fullBufferToSurviveDeletes = newValue;
    }

    /**
     * Allow {@link IndexWriter#addIndexes} to copy indexes from one Directory to another, successfully.
     * <p>
     *     This uses {@link org.apache.lucene.store.Directory#copyFrom}, but {@link org.apache.lucene.store.MockDirectoryWrapper}
     *     does not pass through to the underlying object, so it just does a raw copy of bytes from one directory to another.
     *     This means that it won't copy anything that we have that is not listed as a file.
     *     Setting this to {@code true} puts in a hack that will copy the key-value pairs between directories when the
     *     segment is copied.
     * </p>
     * <p>
     *     Note: the fact that {@link org.apache.lucene.store.MockDirectoryWrapper} does not pass through to the
     *     underlying directory seems intentional, see
     *     <a href="https://issues.apache.org/jira/browse/LUCENE-6299">LUCENE-6299</a> and
     *     <a href="https://github.com/apache/lucene/commit/4e53fae38e993fad0c8a5466e1e0c6121d49ad5f#diff-6c29e90918d4bab6eab63cb94f9c0ae55fb14bbbb07446ee0568bb33343947f7">
     *         commit</a>
     * </p>
     * <p>
     *     If you set this to {@code true} make sure to set it back to {@code false} in a {@code finally} block.
     * </p>
     * @param newValue {@code true} to allow adding indexes.
     */
    public static void setAllowAddIndexes(final boolean newValue) {
        allowAddIndexes = newValue;
        fieldInfosToCopy = null;
    }

    @Nonnull
    @Override
    public IndexInput openInput(@Nonnull final String name, @Nonnull final IOContext ioContext) throws IOException {
        if (fullBufferToSurviveDeletes) {
            try (IndexInput indexInput = super.openInput(name, ioContext)) {
                assertThat("Avoid buffering more than 10MB",
                        indexInput.length(), Matchers.lessThan(10_000_000L));
                final byte[] bytes = new byte[(int)indexInput.length()];
                indexInput.readBytes(bytes, 0, (int)indexInput.length());
                return new ByteBuffersIndexInput(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(bytes))), name);
            }
        }
        if (allowAddIndexes) {
            if (FDBDirectory.isFieldInfoFile(name)) {
                if (isInIndexWriterAddIndexes()) {
                    try (IndexInput fieldInfoInput = super.openInput(name, ioContext)) {
                        fieldInfosToCopy = readFieldInfo(fieldInfoInput.readLong());
                        MatcherAssert.assertThat(fieldInfosToCopy, Matchers.notNullValue());
                    }
                }
            }
        }
        return super.openInput(name, ioContext);
    }

    @Nonnull
    @Override
    public IndexOutput createOutput(@Nonnull final String name, @Nullable final IOContext ioContext) throws IOException {
        if (allowAddIndexes) {
            if (FDBDirectory.isFieldInfoFile(name)) {
                if (isInIndexWriterAddIndexes()) {
                    MatcherAssert.assertThat(fieldInfosToCopy, Matchers.notNullValue());
                    try (IndexOutput indexOutput = super.createOutput(name, ioContext)) {
                        final long id = writeFieldInfo(fieldInfosToCopy);
                        indexOutput.writeLong(id);
                    }
                    fieldInfosToCopy = null;
                    return new ByteBuffersIndexOutput(new ByteBuffersDataOutput(8), name, name);
                }
            }

        }
        return super.createOutput(name, ioContext);
    }

    private static boolean isInIndexWriterAddIndexes() {
        boolean foundAddIndexes = false;
        boolean foundCopyFrom = false;
        for (final StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            if (stackTraceElement.getClassName().equals(IndexWriter.class.getName()) &&
                stackTraceElement.getMethodName().equals("addIndexes")) {
                foundAddIndexes = true;
                if (foundCopyFrom) {
                    return true;
                }
            }
            if (stackTraceElement.getClassName().equals(Directory.class.getName()) &&
                stackTraceElement.getMethodName().equals("copyFrom")) {
                foundCopyFrom = true;
                if (foundAddIndexes) {
                    return true;
                }
            }
        }
        return false;
    }
}
