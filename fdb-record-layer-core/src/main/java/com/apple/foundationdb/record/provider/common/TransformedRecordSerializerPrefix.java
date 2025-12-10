/*
 * TransformedRecordSerializerPrefix.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * Helper class for {@link TransformedRecordSerializer} giving the low-level bit encoding.
 *
 * <p>
 * The format is required to be compatible with various points in the history, which means it
 * must read old data compatibly. Specifically,
 * <ul>
 * <li>use of just {@link DynamicMessageRecordSerializer}, so that Protobuf begins immediately,
 * with no prefix</li>
 * <li>single byte prefix with no support for key identification and rolling</li>
 * </ul>
 * </p>
 *
 * <p>
 * The encoded form begins with a Protobuf <a href="https://protobuf.dev/programming-guides/encoding/#varints">varint</a>.
 * The low three bits of this prefix specify how it was encoded and the remaining bits are the encryption key number.
 * <ul>
 * <li><code>1</code> encrypted</li>
 * <li><code>2</code> clear text or no prefix at all</li>
 * <li><code>4</code> compressed</li>
 * <li><code>5</code> compressed and encrypted (in that order)</li>
 * </ul>
 * Recall that a serialized record is a message using the {@link RecordMetaData#getUnionDescriptor union descriptor}
 * from the {@link RecordMetaData record metadata}. That means this will be wire-type <code>2</code> plus a field number
 * in the union. Since field numbers must be positive, this is unambiguous versus just <code>2</code> for clear text.
 * The remaining prefix types correspond to other Protobuf wire types: <code>1</code> is <code>I64</code>, <code>4</code>
 * is <code>EGROUP</code>, and <code>5</code> is <code>I32</code>. None of these are possible for the <em>start</em> of a
 * serialized message. Finally, a key number of zero becomes a single byte prefix of <code>1</code>, <code>2</code>,
 * <code>4</code> or <code>5</code>, formerly representing an encrypted and compressed bitmask.
 * </p>
 *
 * <p>
 * The encrypted form currently begins with a random IV, although this might change for other algorithms.
 * The compressed form begins with a compression level, which again might someday be extended.
 * </p>
 */
@API(API.Status.INTERNAL)
class TransformedRecordSerializerPrefix {
    protected static final int PREFIX_ENCRYPTED = 1;
    protected static final int PREFIX_CLEAR = 2;
    protected static final int PREFIX_COMPRESSED = 4;
    protected static final int PREFIX_COMPRESSED_THEN_ENCRYPTED = 5;

    protected static final int TYPE_MASK = 0x07;
    protected static final int KEY_SHIFT = 3;

    @SuppressWarnings("fallthrough")
    public static boolean decodePrefix(@Nonnull TransformedRecordSerializerState state, @Nonnull Tuple primaryKey) {
        final long prefix = readVarint(state, primaryKey);
        final int type = (int)(prefix & TYPE_MASK);
        final long remaining = prefix >> KEY_SHIFT;
        if (type == PREFIX_CLEAR && remaining != 0) {
            return false;   // Does not have a prefix
        }
        boolean valid = true;
        switch (type)  {
            case PREFIX_CLEAR:
                break;
            case PREFIX_COMPRESSED_THEN_ENCRYPTED:
                state.setEncrypted(true);
                state.setCompressed(true);
                break;
            case PREFIX_ENCRYPTED:
                state.setEncrypted(true);
                break;
            case PREFIX_COMPRESSED:
                state.setCompressed(true);
                break;
            default:
                valid = false;
                break;
        }
        if (state.isEncrypted()) {
            if (remaining < Integer.MIN_VALUE || remaining > Integer.MAX_VALUE) {
                valid = false;
            } else {
                state.setKeyNumber((int)remaining);
            }
        } else if (remaining != 0) {
            valid = false;
        }
        if (!valid) {
            throw new RecordSerializationException("unrecognized transformation encoding")
                .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey)
                .addLogInfo("encoding", prefix);
        }
        return true;
    }

    public static void encodePrefix(@Nonnull TransformedRecordSerializerState state) {
        long prefix;
        if (!state.isCompressed() && !state.isEncrypted()) {
            prefix = PREFIX_CLEAR;
        } else {
            prefix = 0;
            if (state.isCompressed()) {
                prefix |= PREFIX_COMPRESSED;
            }
            if (state.isEncrypted()) {
                prefix |= PREFIX_ENCRYPTED;
                prefix |= (long)state.getKeyNumber() << KEY_SHIFT;
            }
        }
        int size = state.getLength() + varintSize(prefix);
        byte[] serialized = new byte[size];
        int offset = writeVarint(serialized, prefix);
        System.arraycopy(state.getData(), state.getOffset(), serialized, offset, state.getLength());
        state.setDataArray(serialized);
    }

    protected static int varintSize(long varint) {
        int nbytes = 0;
        do {
            varint >>>= 7;
            nbytes++;
        } while (varint != 0);
        return nbytes;
    }

    protected static long readVarint(@Nonnull TransformedRecordSerializerState state, @Nonnull Tuple primaryKey) {
        long varint = 0;
        int nbytes = 0;
        while (true) {
            if (nbytes >= state.getLength()) {
                throw new RecordSerializationException("transformation prefix malformed")
                        .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
            }
            final byte b = state.getData()[state.getOffset() + nbytes];
            if (nbytes == 9 && (b & 0xFE) != 0) {
                // Continuing or more than just the 64th bit.
                // This also detects random garbage with the sign bits on.
                throw new RecordSerializationException("transformation prefix too long")
                        .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
            }
            varint |= (long)(b & 0x7F) << (nbytes * 7);
            nbytes++;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        state.setOffset(state.getOffset() + nbytes);
        state.setLength(state.getLength() - nbytes);
        return varint;
    }

    protected static int writeVarint(@Nonnull byte[] into, long varint) {
        int nbytes = 0;
        do {
            byte b = (byte)(varint & 0x7F);
            varint >>>= 7;
            if (varint != 0) {
                b |= (byte)0x80;
            }
            into[nbytes] = b;
            nbytes++;
        } while (varint != 0);
        return nbytes;
    }
}
