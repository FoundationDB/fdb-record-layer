/*
 * UUIDUtils.java
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

package com.apple.foundationdb.util;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility methods for working with {@link UUID}s.
 */
public class UUIDUtils {
    private UUIDUtils() {
        // Do not allow instantiation of utility class
    }

    /**
     * Generate a random v4 {@link UUID}. Unlike {@link UUID#randomUUID()}, which
     * uses a cryptographically secure random number generator, this uses a less
     * secure randomness source. In particular, using {@link UUID#randomUUID()} can
     * introduce contention on a lock surrounding the UUID
     *
     * @return a random v4 UUID
     */
    @Nonnull
    public static UUID random() {
        return random(ThreadLocalRandom.current());
    }

    /**
     * Generate a random v4 {@link UUID} using the provided random number generator.
     * This should generate random UUIDs in way that is in accordance with
     * {@link UUID#randomUUID()}, but it allows the user to provide their own
     * random number generator.
     *
     * @param r source of randomness
     * @return a new random v4 UUID
     * @see UUID#randomUUID()
     */
    @Nonnull
    public static UUID random(@Nonnull Random r) {
        long mostSignificantBits = r.nextLong();
        mostSignificantBits &= 0xffffffffffff0fffL; // clear version
        mostSignificantBits |= 0x0000000000004000L; // version 4
        long leastSignificantBits = r.nextLong();
        leastSignificantBits &= 0x3fffffffffffffffL; // clear variant
        leastSignificantBits |= 0x8000000000000000L; // set IETF variant
        return new UUID(mostSignificantBits, leastSignificantBits);
    }
}
