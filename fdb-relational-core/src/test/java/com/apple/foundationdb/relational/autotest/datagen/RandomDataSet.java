/*
 * RandomDataSet.java
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

package com.apple.foundationdb.relational.autotest.datagen;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.autotest.DataSet;

import com.google.protobuf.Message;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

public class RandomDataSet implements DataSet {
    private final long seed;
    private final int maxArraySize;
    private final int numRecords;
    private final int maxStringLength;
    private final int maxBytesLength;

    public RandomDataSet(long seed, int maxArraySize, int numRecords, int maxStringLength, int maxBytesLength) {
        this.seed = seed;
        this.maxArraySize = maxArraySize;
        this.numRecords = numRecords;
        this.maxStringLength = maxStringLength;
        this.maxBytesLength = maxBytesLength;
    }

    @Override
    public Stream<Message> getData(@Nonnull DynamicMessageBuilder messageBuilder) {
        RandomDataSource rds = new UniformDataSource(seed, maxStringLength, maxBytesLength);
        TableDataGenerator tableGenerator = new TableDataGenerator(messageBuilder.getDescriptor(), rds, maxArraySize);

        AtomicInteger counter = new AtomicInteger(0);
        UnaryOperator<Message> dataGenerator = theLast -> {
            try {
                tableGenerator.generateValue(messageBuilder);
                counter.getAndIncrement();
                return messageBuilder.build();
            } catch (RelationalException e) {
                throw e.toUncheckedWrappedException();
            }
        };
        Message first = dataGenerator.apply(null);
        return Stream.iterate(first, message -> counter.get() <= numRecords, dataGenerator);
    }
}
