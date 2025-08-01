/*
 * NodeKind.java
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

import com.google.common.base.Verify;

import javax.annotation.Nonnull;

/**
 * Enum to capture the kind of node.
 */
public enum NodeKind {
    COMPACT((byte)0x00),
    INLINING((byte)0x01);

    private final byte serialized;

    NodeKind(final byte serialized) {
        this.serialized = serialized;
    }

    public byte getSerialized() {
        return serialized;
    }

    @Nonnull
    static NodeKind fromSerializedNodeKind(byte serializedNodeKind) {
        final NodeKind nodeKind;
        switch (serializedNodeKind) {
            case 0x00:
                nodeKind = NodeKind.COMPACT;
                break;
            case 0x01:
                nodeKind = NodeKind.INLINING;
                break;
            default:
                throw new IllegalArgumentException("unknown node kind");
        }
        Verify.verify(nodeKind.getSerialized() == serializedNodeKind);
        return nodeKind;
    }
}
