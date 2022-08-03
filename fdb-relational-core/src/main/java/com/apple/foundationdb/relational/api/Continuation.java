/*
 * Continuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import javax.annotation.Nullable;

public interface Continuation {

    Continuation END = new Continuation() {
        @SuppressWarnings("PMD.FieldNamingConventions")
        private final byte[] empty = new byte[]{};

        @Nullable
        @Override
        public byte[] getBytes() {
            return empty;
        }

        @Override
        public boolean atBeginning() {
            return false;
        }

        @Override
        public boolean atEnd() {
            return true;
        }
    };

    Continuation BEGIN = new Continuation() {
        @Nullable
        @Override
        public byte[] getBytes() {
            return null;
        }

        @Override
        public boolean atBeginning() {
            return true;
        }

        @Override
        public boolean atEnd() {
            return false;
        }
    };

    Continuation EMPTY_SET = new Continuation() {
        @Nullable
        @Override
        public byte[] getBytes() {
            return null;
        }

        @Override
        public boolean atBeginning() {
            return true;
        }

        @Override
        public boolean atEnd() {
            return true;
        }
    };

    /**
     * Get the continuation as a sequence of bytes.
     *
     * @return the continuation as a byte array. If the returned array is {@code null}, then this continuation
     * is treated as being at the <em>beginning</em> (equivalent to {@link #BEGIN}). If the returned array is empty
     * then this should be treated as equivalent to {@link #END}.
     */
    @Nullable
    byte[] getBytes();

    default boolean atBeginning() {
        return getBytes() == null;
    }

    default boolean atEnd() {
        byte[] bytes = getBytes();
        return bytes != null && bytes.length == 0;
    }
}
