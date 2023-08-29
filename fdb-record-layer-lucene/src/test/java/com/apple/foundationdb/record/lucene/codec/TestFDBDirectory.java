package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TestFDBDirectory extends FDBDirectory {
    private static AtomicReference<FDBDirectory> lastDir = new AtomicReference<>();

    public TestFDBDirectory() {
        super(new Subspace(Tuple.from("record-test", "unit", "lucene")),
                FDBDatabaseFactory.instance().getDatabase().openContext());
    }

    @Nonnull
    @Override
    public IndexOutput createOutput(@Nonnull final String name, @Nullable final IOContext ioContext) {
        final IndexOutput output = super.createOutput(name, ioContext);
        if (isFieldInfoFile(name)) {
            return new WrappedOutput(this, lastDir.get(), name, output);
        } else {
            return output;
        }
    }

    @Nonnull
    @Override
    public IndexInput openInput(@Nonnull final String name, @Nonnull final IOContext ioContext) throws IOException {
        lastDir.set(this);
        return super.openInput(name, ioContext);
    }

    private class WrappedOutput extends IndexOutput {
        private final FDBDirectory thisDirectory;
        private final FDBDirectory otherDirectory;
        private final IndexOutput underlying;

        public WrappedOutput(final FDBDirectory thisDirectory, final FDBDirectory otherDirectory,
                             final String name, final IndexOutput underlying) {
            super(name, name);
            this.thisDirectory = thisDirectory;
            this.otherDirectory = otherDirectory;
            this.underlying = underlying;
        }



        @Override
        public void copyBytes(final DataInput input, final long numBytes) throws IOException {
            underlying.copyBytes(input, numBytes);
            // TOTAL Hack
            // Some of lucene's tests (e.g. BaseStoredFieldsFormatTestCase.testMismatchedFields) use
            // IndexWriter.addIndexes to copy all segmented data. This doesn't copy anything that is not segmented,
            // and as far as I can tell, there is no way to copy non-segmented data.
            // In theory we could use Directory.copyFrom, and when we're copying FieldInfo copy the associated
            // un-segmented data, but MockDirectoryWrapper does not call underlying.copyFrom, it calls super.copyFrom
            // so our override wouldn't be called at all.
            // We could, in theory we could have our IndexInput override this normally and deal with stuff, but we get
            // an arbitrarily wrapped DataInput, and no way (AFAICT) to get to the underlying FDB input, or directory.
            // To work around this, we have a hack here where we track what the last directory opened for input was and
            // read from that.

            for (final KeyValue keyValue : lastDir.get().getContext().ensureActive().getRange(schemaSubspace.range())) {
                getContext().ensureActive().set(keyValue.getKey(), keyValue.getValue());
            }
        }

        @Override
        public String getName() {
            return underlying.getName();
        }

        @Override
        public String toString() {
            return underlying.toString();
        }

        @Override
        public void writeBytes(final byte[] b, final int length) throws IOException {
            underlying.writeBytes(b, length);
        }

        @Override
        public void writeInt(final int i) throws IOException {
            underlying.writeInt(i);
        }

        @Override
        public void writeShort(final short i) throws IOException {
            underlying.writeShort(i);
        }

        @Override
        public void writeLong(final long i) throws IOException {
            underlying.writeLong(i);
        }

        @Override
        public void writeString(final String s) throws IOException {
            underlying.writeString(s);
        }

        @Override
        public void writeMapOfStrings(final Map<String, String> map) throws IOException {
            underlying.writeMapOfStrings(map);
        }

        @Override
        public void writeSetOfStrings(final Set<String> set) throws IOException {
            underlying.writeSetOfStrings(set);
        }

        @Override
        public void close() throws IOException {
            underlying.close();
        }

        @Override
        public long getFilePointer() {
            return underlying.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return underlying.getChecksum();
        }

        @Override
        public void writeByte(final byte b) throws IOException {
            underlying.writeByte(b);
        }

        @Override
        public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
            underlying.writeBytes(b, offset, length);
        }
    }
}
