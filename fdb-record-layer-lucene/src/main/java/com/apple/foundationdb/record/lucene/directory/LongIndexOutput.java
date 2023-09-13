package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongConsumer;

public class LongIndexOutput extends IndexOutput {

    private final LongConsumer applyLong;
    private ByteBuffer buffer;
    boolean written = false;

    /**
     * Sole constructor.  resourceDescription should be non-null, opaque string
     * describing this resource; it's returned from {@link #toString}.
     */
    protected LongIndexOutput(final String resourceDescription, final String name, LongConsumer applyLong) {
        super(resourceDescription, name);
        this.applyLong = applyLong;
        buffer = ByteBuffer.allocate(Long.BYTES);
    }

    @Override
    public void close() throws IOException {
        if (buffer.position() != Long.BYTES) {
            throw new RecordCoreException("Tried to close after insufficient bytes")
                    .addLogInfo(LogMessageKeys.VALUE_SIZE, buffer.position());
        }
        buffer.flip();
        applyLong.accept(buffer.getLong());
    }

    @Override
    public long getFilePointer() {
        return 0;
    }

    @Override
    public long getChecksum() throws IOException {
        return 0;
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        buffer.put(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        buffer.put(b, offset, length);
    }
}
