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

import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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

    /**
     * Helper method to format the node ids of an insert/update path as a string.
     * @param node a node that is usually linked up to its parents to form an insert/update path
     * @return a {@link String} containing the string presentation of the insert/update path starting at {@code node}
     */
    @Nonnull
    static String nodeIdPath(@Nullable Node node) {
        final List<String> nodeIds = Lists.newArrayList();
        do {
            if (node != null) {
                nodeIds.add(bytesToHex(node.getId()));
                node = node.getParentNode();
            } else {
                nodeIds.add("<null>");
            }
        } while (node != null);
        Collections.reverse(nodeIds);
        return String.join(", ", nodeIds);
    }
}
