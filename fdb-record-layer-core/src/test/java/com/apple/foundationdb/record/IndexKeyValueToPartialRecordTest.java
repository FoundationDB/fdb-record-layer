/*
 * IndexKeyValueToPartialRecordTest.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class IndexKeyValueToPartialRecordTest {

    @Nonnull
    private static final Random random = new Random();

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    private static final IndexKeyValueToPartialRecord plan = IndexKeyValueToPartialRecord.newBuilder(TestRecords1Proto.MySimpleRecord.getDescriptor())
            .addField("num_value_2", IndexKeyValueToPartialRecord.TupleSource.VALUE, new AvailableFields.TruePredicate(), ImmutableIntArray.of(0), null)
            .build();

    @Nonnull
    private static Descriptors.Descriptor evolveMessage(@Nonnull final Descriptors.Descriptor originalDescriptor) {
        final var descriptorWithNewFieldProto = DescriptorProtos.DescriptorProto.newBuilder(originalDescriptor.toProto())
                .addField(DescriptorProtos.FieldDescriptorProto
                        .newBuilder()
                        .setName("NewField")
                        .setNumber(100)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
                        .build())
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder().setName("synthetic.proto").addMessageType(descriptorWithNewFieldProto).build();
        final Descriptors.FileDescriptor descriptorWithNewField;
        try {
            descriptorWithNewField = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Could not construct descriptor from synthetic descriptor proto for " + originalDescriptor.getName(), e);
        }
        return descriptorWithNewField.findMessageTypeByName(descriptorWithNewFieldProto.getName());
    }

    @Nonnull
    private static final Descriptors.Descriptor originalDescriptor = TestRecords1Proto.MySimpleRecord.getDescriptor();

    @Nonnull
    private static final Descriptors.Descriptor newDescriptor = evolveMessage(originalDescriptor);

    @Nonnull
    private static IndexEntry randomIndexEntry() {
        return new IndexEntry(new Index("foo", "bar"), Tuple.from("num_value_2"), Tuple.from(42L));
    }

    void executePlan() {
        final var tossedCoinIsTails = random.nextBoolean();
        final var chosenDescriptor = tossedCoinIsTails ? originalDescriptor : newDescriptor;
        plan.toRecord(chosenDescriptor, randomIndexEntry());
    }

    @Test
    void convertIndexToPartialRecordConcurrently() throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        int numTasks = 1000;
        CountDownLatch latch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            service.submit(() -> {
                try {
                    executePlan();
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
        }
        boolean completed = latch.await(2L, TimeUnit.SECONDS);
        service.shutdown();
        Assertions.assertTrue(completed);
    }

    @Test
    void testAddRequiredMessage() {
        // has a required message field in a nested field
        IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(TestRequiredNestedRecordProto.MyNestedRecord.getDescriptor());
        builder.addRequiredMessageFields();
        Assertions.assertTrue(builder.isValid(true));
    }
}
