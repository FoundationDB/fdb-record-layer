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

import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;

/**
 * Utility functions and constants for tests involving full text queries.
 */
public class TextIndexTestUtils {
    public static final String SIMPLE_DOC = "SimpleDocument";
    public static final String COMPLEX_DOC = "ComplexDocument";
    public static final String MAP_DOC = "MapDocument";
    public static final String MULTI_DOC = "MultiDocument";
    public static final String MANY_FIELDS_DOC = "ManyFieldsDocument";
    public static final List<String> ALL_DOC_TYPES = List.of(SIMPLE_DOC, COMPLEX_DOC, MAP_DOC, MULTI_DOC, MANY_FIELDS_DOC);
    public static final TransformedRecordSerializer<Message> COMPRESSING_SERIALIZER =
            TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
    public static final String SIMPLE_DEFAULT_NAME = "SimpleDocument$text";

    @Nonnull
    public static List<TestRecordsTextProto.SimpleDocument> toSimpleDocuments(@Nonnull List<String> textSamples) {
        return IntStream.range(0, textSamples.size())
                .mapToObj(i -> TestRecordsTextProto.SimpleDocument.newBuilder().setDocId(i).setGroup(i % 2).setText(textSamples.get(i)).build())
                .collect(Collectors.toList());
    }

    /**
     * Adds a record type key prefix to each primary key. This is useful for text index tests, as it makes the
     * text index types behave more like relational database tables, which in turn enables things like single-
     * type {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#deleteRecordsWhere(QueryComponent) deleteRecordsWhere()}
     * operations.
     *
     * @param metaDataBuilder the meta-data builder to add record type prefixes for each document type
     */
    public static void addRecordTypePrefix(RecordMetaDataBuilder metaDataBuilder) {
        for (String type : ALL_DOC_TYPES) {
            final RecordTypeBuilder typeBuilder = metaDataBuilder.getRecordType(type);
            typeBuilder.setPrimaryKey(concat(recordType(), typeBuilder.getPrimaryKey()));
        }
    }

    private TextIndexTestUtils() {
        // Container class that should not be instantiated.
    }
}
