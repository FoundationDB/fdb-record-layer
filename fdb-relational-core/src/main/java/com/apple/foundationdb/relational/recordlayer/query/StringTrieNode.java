/*
 * StringTrieNode.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.util.TrieNode;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@SpotBugsSuppressWarnings(value = "SING_SINGLETON_HAS_NONPRIVATE_CONSTRUCTOR", justification = "Singleton designation is a false positive")
@API(API.Status.EXPERIMENTAL)
public class StringTrieNode extends TrieNode.AbstractTrieNode<String, Void, StringTrieNode> {
    @Nonnull
    private static final StringTrieNode LEAF = new StringTrieNode(null);

    public StringTrieNode(@Nullable Map<String, StringTrieNode> childrenMap) {
        this(null, childrenMap);
    }

    public StringTrieNode(@Nullable Void value, @Nullable Map<String, StringTrieNode> childrenMap) {
        super(value, childrenMap);
    }

    @Nonnull
    @Override
    public StringTrieNode getThis() {
        return this;
    }

    @Nonnull
    public static StringTrieNode leafNode() {
        return LEAF;
    }
}
