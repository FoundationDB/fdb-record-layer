/*
 * InMemoryTable.java
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

package com.apple.foundationdb.relational.memory;

import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecordBuilder;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class InMemoryTable {
    private final RecordType recordType;
    private final ConcurrentNavigableMap<byte[], Message> data = new ConcurrentSkipListMap<>(ByteArrayUtil.comparator());

    public InMemoryTable(RecordType recordType) {
        this.recordType = recordType;
    }

    public int add(Iterator<? extends Message> messages) throws RelationalException {
        try {
            AtomicInteger count = new AtomicInteger(0);
            KeyExpression keyFunc = recordType.getPrimaryKey();
            messages.forEachRemaining((Consumer<Message>) message -> {
                FDBStoredRecordBuilder<Message> rec = new FDBStoredRecordBuilder<>().setRecord(message).setRecordType(recordType);
                byte[] key = keyFunc.evaluateSingleton(rec).toTuple().pack();
                Message old = data.putIfAbsent(key, message);
                if (old != null) {
                    try {
                        throw new RelationalException("Duplicate key for message " + message, ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
                    } catch (RelationalException e) {
                        throw e.toUncheckedWrappedException();
                    }
                }
                count.incrementAndGet();
            });
            return count.get();
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        }
    }

    public Message get(KeySet key) {
        Map<String, Object> keys = key.toMap();
        return data.values().stream().filter(row -> {
            boolean equals = true;
            for (Map.Entry<String, Object> entry : keys.entrySet()) {
                for (Descriptors.FieldDescriptor fd :row.getDescriptorForType().getFields()) {
                    if (fd.getName().equalsIgnoreCase(entry.getKey())) {
                        if (!Objects.equals(entry.getValue(), row.getField(fd))) {
                            equals = false;
                            break;
                        }
                    }
                }
            }
            return equals;
        }).findFirst().orElse(null);
    }

    public Stream<Message> scan(Map<String, Object> startKey, Map<String, Object> endKey) throws RelationalException {
        try {
            byte[] start = getPrimaryKey(startKey);
            byte[] end = getPrimaryKey(endKey);

            final ConcurrentNavigableMap<byte[], Message> subMap = data.subMap(start, true, end, true);
            return subMap.values().stream();

        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        }
    }

    private byte[] getPrimaryKey(Map<String, Object> startKey) {
        DynamicMessageBuilder startDmb = new ProtobufDataBuilder(recordType.getDescriptor());
        startKey.forEach((key, value) -> {
            for (Descriptors.FieldDescriptor fd : getDescriptor().getFields()) {
                if (fd.getName().equalsIgnoreCase(key)) {
                    try {
                        startDmb.setField(fd.getName(), value);
                        break;
                    } catch (RelationalException e) {
                        throw e.toUncheckedWrappedException();
                    }
                }
            }
        });
        FDBStoredRecordBuilder<Message> rec = new FDBStoredRecordBuilder<>().setRecord(startDmb.build()).setRecordType(recordType);

        return recordType.getPrimaryKey().evaluateSingleton(rec).toTuple().pack();
    }

    public Descriptors.Descriptor getDescriptor() {
        return recordType.getDescriptor();
    }

}
