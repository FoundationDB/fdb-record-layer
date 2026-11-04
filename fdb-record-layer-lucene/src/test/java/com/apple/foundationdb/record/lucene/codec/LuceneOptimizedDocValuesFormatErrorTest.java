/*
 * LuceneOptimizedDocValuesFormatErrorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests how {@link LuceneOptimizedDocValuesFormat} reports failures to open the underlying doc values producer.
 */
class LuceneOptimizedDocValuesFormatErrorTest {

    /**
     * {@code Lucene80DocValuesProducer} calls {@code CodecUtil.checkFooter} even if reading the header failed, so an
     * FDB error (like a timeout) during the header read comes out as a {@link CorruptIndexException} that only has
     * the FDB error as a suppressed exception. Make sure the FDB error is the cause, so that callers do not
     * mistake it for corruption.
     */
    @Test
    void suppressedRuntimeExceptionBecomesCause() {
        final RecordCoreException timeout = new RecordCoreException("Timeout waiting for asynchronous operation to complete");
        final CorruptIndexException corrupt = new CorruptIndexException("checksum failed (hardware problem?)", "test");
        corrupt.addSuppressed(timeout);

        final IOException thrown = assertThrows(IOException.class, () -> invokeProducerCall(corrupt));

        assertSame(timeout, thrown.getCause());
        assertTrue(Arrays.asList(thrown.getSuppressed()).contains(corrupt));
    }

    @Test
    void genuineCorruptionHasNoCause() {
        final CorruptIndexException corrupt = new CorruptIndexException("checksum failed (hardware problem?)", "test");

        final CorruptIndexException thrown = assertThrows(CorruptIndexException.class, () -> invokeProducerCall(corrupt));

        assertSame(corrupt, thrown);
        assertNull(thrown.getCause());
    }

    private static void invokeProducerCall(final IOException failure) throws IOException {
        final DocValuesProducer producer = new LuceneOptimizedDocValuesFormat(new FailingDocValuesFormat(failure))
                .fieldsProducer(null);
        producer.getSorted(null);
    }

    private static class FailingDocValuesFormat extends DocValuesFormat {
        private final IOException failure;

        FailingDocValuesFormat(final IOException failure) {
            super("Failing");
            this.failure = failure;
        }

        @Override
        public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocValuesProducer fieldsProducer(final SegmentReadState state) throws IOException {
            throw failure;
        }
    }
}
