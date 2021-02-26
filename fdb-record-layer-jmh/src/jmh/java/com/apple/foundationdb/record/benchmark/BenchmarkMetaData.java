/*
 * FDBRecordStoreBenchmark.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark;

import com.apple.foundationdb.record.BenchmarkRecords1Proto;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;

import java.util.Collections;

/**
 * Methods for generating benchmark {@link RecordMetaData}.
 */
public class BenchmarkMetaData {
    private BenchmarkMetaData() {
    }

    /**
     * No additional indexes.
     * @return records #1 base meta-data
     */
    public static RecordMetaDataBuilder records1Base() {
        return RecordMetaData.newBuilder().setRecords(BenchmarkRecords1Proto.getDescriptor());
    }

    /**
     * Add a {@code RANK} index.
     * @return records #1 meta-data with added rank index
     */
    public static RecordMetaDataBuilder records1Rank() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(BenchmarkRecords1Proto.getDescriptor());
        builder.removeIndex("MySimpleRecord$num_value_unique");
        builder.addIndex("MySimpleRecord", new Index("num_value_unique_rank", Key.Expressions.field("num_value_unique").ungrouped(),
                EmptyKeyExpression.EMPTY, IndexTypes.RANK, Collections.emptyMap()));
        return builder;
    }

}
