/*
 * NodeHelpers.java
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

package com.apple.foundationdb.async.hnsw;

import javax.annotation.Nonnull;

/**
 * Some helper methods for {@link Node}s.
 */
public class NodeHelpers {
    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    private NodeHelpers() {
        // nothing
    }

    /**
     * Helper method to format bytes as hex strings for logging and debugging.
     * @param bytes an array of bytes
     * @return a {@link String} containing the hexadecimal representation of the byte array passed in
     */
    @Nonnull
    static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return "0x" + new String(hexChars).replaceFirst("^0+(?!$)", "");
    }
}
