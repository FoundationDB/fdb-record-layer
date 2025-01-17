/*
 * PositionalIndex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.annotation.API;

/**
 * The field positional offset/index is zero-based in protobuf but one-based in JDBC. Below is small utility
 * for going between the different indexing modes.
 */
@API(API.Status.EXPERIMENTAL)
public class PositionalIndex {
    /**
     * Adjust 'oneBasedIndex' so can be used as a zero-based protobuf index.
     * @param oneBasedIndex JDBC indices are oneBased; i.e. first item is at postion 1.
     * @return Protobuf index derived from <code>oneBasedIndex</code>.
     */
    public static int toProtobuf(int oneBasedIndex) {
        return oneBasedIndex - 1;
    }

    /**
     * Adjust 'zeroBasedIndex' so can be used as a one-based JDBC index.
     * @param zeroBasedProtobufIndex Protobuf zero-based index.
     * @return An index that can be used on JDBC types.
     */
    public static int toJDBC(int zeroBasedProtobufIndex) {
        return zeroBasedProtobufIndex + 1;
    }
}
