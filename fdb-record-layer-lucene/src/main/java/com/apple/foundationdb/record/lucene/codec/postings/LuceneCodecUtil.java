/*
 * LuceneCodecUtil.java
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

package com.apple.foundationdb.record.lucene.codec.postings;

import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCompoundReader;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedWrappedDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.BytesRef;

public class LuceneCodecUtil {

    public static Tuple BytesRefToTuple(Tuple tuple, BytesRef text) {
        assert (text != null): "Text cannot be null";
        return tuple.add(text.bytes, text.offset, text.length);
    }

    public static BytesRef BytesStringToBytesRef(ByteString byteString) {
        return new BytesRef(byteString.toByteArray());
    }

    public static FDBDirectory unwrapDirectory(Directory directory) {
        Directory delegate = FilterDirectory.unwrap(directory);
        if (delegate instanceof LuceneOptimizedCompoundReader) {
            delegate = ((LuceneOptimizedCompoundReader) delegate).getDirectory();
        }
        if (delegate instanceof LuceneOptimizedWrappedDirectory) {
            delegate = ((LuceneOptimizedWrappedDirectory) delegate).getFdbDirectory();
        }
        if (delegate instanceof FDBDirectory) {
            return (FDBDirectory) delegate;
        } else {
            throw new RuntimeException("Expected FDB Directory " + delegate.getClass());
        }
    }

}
