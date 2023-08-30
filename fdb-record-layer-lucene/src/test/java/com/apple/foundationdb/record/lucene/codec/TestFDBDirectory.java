package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.hamcrest.Matchers;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class TestFDBDirectory extends FDBDirectory {
    private static boolean fullBufferToSurviveDeletes;

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
        return indexInput;
    }

}
