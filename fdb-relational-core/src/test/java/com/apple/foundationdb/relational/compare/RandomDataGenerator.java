/*
 * RandomDataGenerator.java
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

package com.apple.foundationdb.relational.compare;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

/**
 *  Generates random data to fill all the fields in a specific data type, recursively generating random
 *  values to fill nested fields. You can use `numRepeatedFields` to set a limit on how many repeated entries
 *  will be randomly generated
 */
public class RandomDataGenerator implements DataGenerator {
    private final Random random;
    private final Descriptors.Descriptor dataType;
    private final int maxRepeatedFields;
    private final int numRecordsToGenerate;
    private final int maxByteLength; //the maximum length to populate blobs for. May be shorter though (depends on the random)

    private int generatedRecords;
    private final int maxStringLength;

    public RandomDataGenerator(Random random,
                               Descriptors.Descriptor dataType,
                               int maxRepeatedFields,
                               int numRecordsToGenerate,
                               int maxByteLength,
                               int maxStringLength) {
        this.random = random;
        this.dataType = dataType;
        this.maxRepeatedFields = maxRepeatedFields;
        this.numRecordsToGenerate = numRecordsToGenerate;
        this.maxByteLength = maxByteLength;
        this.maxStringLength = maxStringLength;
    }

    /**
     * Get the next randomly generated message.
     *
     * @return the next randomly generated message, or {@code null} if the generator is exhausted
     */
    @Override
    @Nullable
    public Message generateNext() {
        if (generatedRecords >= numRecordsToGenerate) {
            return null;
        }
        generatedRecords++;
        return generateRecord(dataType);
    }

    private Message generateRecord(Descriptors.Descriptor type) {
        DynamicMessage.Builder topLevel = DynamicMessage.newBuilder(type);
        for (Descriptors.FieldDescriptor fd : type.getFields()) {
            if (fd.isRepeated()) {
                int numRepeated = random.nextInt(maxRepeatedFields);
                for (int i = 0; i < numRepeated; i++) {
                    Object o = generateField(fd);
                    if (o != null) {
                        topLevel.addRepeatedField(fd, o);
                    }
                }
            } else {
                Object o = generateField(fd);
                if (o != null) {
                    topLevel.setField(fd, o);
                }
            }
        }
        return topLevel.build();
    }

    @Nullable
    private Object generateField(Descriptors.FieldDescriptor field) {
        Object o = null;
        switch (field.getJavaType()) {
            case INT:
                o =  random.nextInt();
                break;
            case LONG:
                o =  random.nextLong();
                break;
            case FLOAT:
                o =  random.nextFloat();
                break;
            case DOUBLE:
                o =  random.nextDouble();
                break;
            case BOOLEAN:
                o =  random.nextBoolean();
                break;
            case STRING:
                o =  makeString();
                break;
            case BYTE_STRING:
                o =  ByteString.copyFrom(makeBytes());
                break;
            case MESSAGE:
                o =  generateRecord(field.getMessageType());
                break;
            case ENUM:
                final List<Descriptors.EnumValueDescriptor> values = field.getEnumType().getValues();
                int enumPos = random.nextInt(values.size());
                //TODO(bfines) not sure if this is correct or not
                o = values.get(enumPos);
                break;
            default:
                throw new IllegalStateException("Unepxected java type <" + field.getJavaType() + ">");
        }
        return o;
    }

    private byte[] makeBytes() {
        byte[] bytes = new byte[random.nextInt(maxByteLength)];
        random.nextBytes(bytes);
        return bytes;
    }

    /*
     *  Generates a random string consisting only of valid UTF-8 characters. Why? Because I like strings without
     *  weird "symbol missing" errors, that's why. Don't Judge me
     */
    private String makeString() {
        int stringSize = random.nextInt(maxStringLength);
        StringBuilder sb = new StringBuilder();
        byte[] data = new byte[4];
        int size;
        while (sb.length() < stringSize) {
            size = -1;
            int fb = random.nextInt(256);
            if (fb < 0x00000080) {
                data[0] = (byte) fb;
                size = 1;
            } else if (fb >= 0x000000C2 && fb < 0x000000E0) {
                size = 2;
                data[0] = (byte) fb;
                data[1] = randomByte(0x80, 0xC0);
            } else if (fb == 0x000000E0) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = randomByte(0xA0, 0xC0);
                data[2] = randomByte(0x80, 0xC0);
            } else if (fb >= 0x000000E1 && fb < 0x000000ED) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = randomByte(0x80, 0xC0);
                data[2] = randomByte(0x80, 0xC0);
            } else if (fb == 0xED) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = randomByte(0x80, 0xA0);
                data[2] = randomByte(0x80, 0xC0);
            } else if (fb >= 0xEE && fb < 0xF0) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = randomByte(0x80, 0xA0);
                data[2] = randomByte(0x80, 0xC0);
            } else if (fb == 0xF0) {
                size = 4;
                data[0] = (byte) fb;
                data[1] = randomByte(0x90, 0xC0);
                data[2] = randomByte(0x80, 0xC0);
                data[3] = randomByte(0x80, 0xC0);
            } else if (fb >= 0xF1 && fb < 0xF4) {
                size = 4;
                data[0] = (byte) fb;
                data[1] = randomByte(0x80, 0xC0);
                data[2] = randomByte(0x80, 0xC0);
                data[3] = randomByte(0x80, 0xC0);
            } else if (fb == 0xF4) {
                size = 4;
                data[0] = (byte) fb;
                data[1] = randomByte(0x80, 0x9F);
                data[2] = randomByte(0x80, 0xC0);
                data[3] = randomByte(0x80, 0xC0);
            }

            if (size > 0) {
                sb.append(new String(data, 0, size, StandardCharsets.UTF_8));
            }
        }
        return sb.toString();
    }

    byte randomByte(int lowByte, int highByte) {
        //make the bounds unsigned, so that we can do the math properly
        int min = lowByte & 0xFF;
        int max = highByte & 0xFF;
        return (byte) (random.nextInt(max - min) + min);
    }

}
