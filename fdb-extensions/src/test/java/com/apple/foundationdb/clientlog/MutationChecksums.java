/*
 * MutationChecksums.java
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

package com.apple.foundationdb.clientlog;

import javax.annotation.Nonnull;

/**
 * An independent implementation of the checksum that FDB attaches to mutations (protocol version 7.4 and later), so
 * that tests can check what was decoded rather than trusting the decoder.
 *
 * <p>FDB's {@code MutationRef::populateChecksum} seeds a CRC32C with the mutation type and then appends both
 * parameters, where the parameters are those of the mutation before serialization. In particular, a clear of a
 * single key is checksummed as the range from the key to the key followed by a zero byte, not in the compact form
 * used on the wire.
 */
final class MutationChecksums {
    // The reversed form of the CRC32C (Castagnoli) polynomial, which is how it is used when going a bit at a time
    // from the low end.
    private static final int POLYNOMIAL = 0x82F63B78;

    private MutationChecksums() {
    }

    /**
     * Continue a CRC32C from a previous value, the way {@code crc32c_append} does. The JDK's {@link java.util.zip.CRC32C}
     * is not used because it cannot be started from a given value, and that is what FDB does with the type of the
     * mutation.
     *
     * @param crc the previous value, or a seed
     * @param data the data to append
     * @return the new value
     */
    static int append(int crc, @Nonnull byte[] data) {
        int state = ~crc;
        for (byte b : data) {
            state ^= b & 0xFF;
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                state = (state & 1) != 0 ? (state >>> 1) ^ POLYNOMIAL : state >>> 1;
            }
        }
        return ~state;
    }

    /**
     * Compute the checksum of a mutation.
     *
     * @param mutation the mutation as it was before it was serialized
     * @return the checksum that FDB would have attached
     */
    static int forMutation(@Nonnull FDBClientLogEvents.Mutation mutation) {
        return append(append(mutation.getType(), mutation.getKey()), mutation.getParam());
    }
}
