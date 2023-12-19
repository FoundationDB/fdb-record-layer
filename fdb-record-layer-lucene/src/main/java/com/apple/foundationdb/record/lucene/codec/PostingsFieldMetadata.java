/*
 * PostingsFieldMetadata.java
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

package com.apple.foundationdb.record.lucene.codec;

import org.apache.lucene.util.BytesRef;

public class PostingsFieldMetadata {
    public PostingsFieldMetadata(final byte[] bytes) {
    }

    public BytesRef getMinTerm() {
        return null;
    }

    public BytesRef getMaxTerm() {
        return null;
    }

    public long getNumTerms() {
        return 0;
    }

    public long getSumTotalTermFreq() {
        return 0;
    }

    public long getSumDocFreq() {
        return 0;
    }

    public int maxDocdoc() {
        return 0;
    }

}
