/*
 * TextIndexTestUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility functions and constants for tests involving full text queries.
 */
public class TextIndexTestUtils {
    public static final String SIMPLE_DOC = "SimpleDocument";
    public static final String COMPLEX_DOC = "ComplexDocument";
    public static final TransformedRecordSerializer<Message> COMPRESSING_SERIALIZER =
            TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
    public static final String SIMPLE_DEFAULT_NAME = "SimpleDocument$text";
    public static final String COMPLEX_DEFAULT_NAME = "Complex$text_index";

    @Nonnull
    public static List<TestRecordsTextProto.SimpleDocument> toSimpleDocuments(@Nonnull List<String> textSamples) {
        return IntStream.range(0, textSamples.size())
                .mapToObj(i -> TestRecordsTextProto.SimpleDocument.newBuilder().setDocId(i).setGroup(i % 2).setText(textSamples.get(i)).build())
                .collect(Collectors.toList());
    }

    private TextIndexTestUtils() {
        // Container class that should not be instantiated.
    }
}
