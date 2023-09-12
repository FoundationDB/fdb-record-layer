package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LongIndexInput extends IndexInput {

    private final ByteBuffer buffer;

    /**
     * resourceDescription should be a non-null, opaque string
     * describing this resource; it's returned from
     * {@link #toString}.
     */
    public LongIndexInput(final String resourceDescription, long longValue) {
        super(resourceDescription);
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(longValue);
        buffer.flip();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long readLong() throws IOException {
        return buffer.getLong();
    }

    @Override
    public long getFilePointer() {
        return 0;
    }

    @Override
    public void seek(final long pos) throws IOException {
        throw new RecordCoreException("Tried to seek from LongIndexInput")
                .addLogInfo(LuceneLogMessageKeys.RESOURCE, this);
    }

    @Override
    public long length() {
        return Long.BYTES;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        throw new RecordCoreException("Tried to slice from LongIndexInput")
                .addLogInfo(LuceneLogMessageKeys.RESOURCE, this);
    }

    @Override
    public byte readByte() throws IOException {
        return buffer.get();
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        buffer.get(b, offset, len);
    }
}
