/*
 * LuceneIndexKeySerializer.java
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

package com.apple.foundationdb.record.lucene.idformat;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LuceneIndexKeySerializer {
    // The size of each of the dimensions. 9 bytes is enough for a Tuple-encoded INT64, so using this size would reduce
    // the overall waste of multiple dimensions of various sizes (INT64 is a common size to be used).
    public static final int BINARY_POINT_DIMENSION_SIZE = 9;

    @Nullable
    private RecordIdFormat format;
    @Nonnull
    private Tuple key;

    public LuceneIndexKeySerializer(@Nullable RecordIdFormat format, @Nonnull final Tuple key) {
        this.key = key;
        this.format = format;
    }

    public static LuceneIndexKeySerializer fromStringFormat(@Nullable final String formatString, @Nonnull final Tuple key) {
        RecordIdFormat format = (formatString == null) ? null : RecordIdFormatParser.parse(formatString);
        return new LuceneIndexKeySerializer(format, key);
    }

    public byte[] asPackedByteArray() {
        return key.pack();
    }

    public byte[][] asPackedBinaryPoint() {
        // We might use a different dimension size here
        List<byte[]> splitBytes = split(key.pack(), BINARY_POINT_DIMENSION_SIZE);
        return splitBytes.toArray(new byte[splitBytes.size()][]);
    }

    public byte[][] asFormattedBinaryPoint() throws RecordCoreArgumentException {
        // No format means use packed byte array
        // TODO: Is this a good idea?
        if (format == null) {
            return asPackedBinaryPoint();
        } else {
            List<byte[]> formattedBytes = applyFormat(format.getElement(), key);
            List<byte[]> splitBytes = splitAll(formattedBytes);
            return splitBytes.toArray(new byte[splitBytes.size()][]);
        }
    }

    private static List<byte[]> splitAll(List<byte[]> byteArrayList) {
        return byteArrayList.stream().flatMap(byteArray -> {
            if (byteArray.length == BINARY_POINT_DIMENSION_SIZE) {
                // no need to copy bytes
                return Stream.of(byteArray);
            } else {
                return split(byteArray, BINARY_POINT_DIMENSION_SIZE).stream();
            }
        }).collect(Collectors.toList());
    }

    // package protected for testing
    // test shorter, equal, multiply and multiply+remainder
    static List<byte[]> split(byte[] source, int splitLength) {
        int numSubArrays = source.length / splitLength;
        if ((source.length % splitLength) != 0) {
            numSubArrays += 1;
        }
        List<byte[]> result = new ArrayList<>(numSubArrays);
        for (int i = 0; i < numSubArrays; i++) {
            // This would pad the last element with 0, which is OK since the packed tuple has a known number of bytes
            result.add(Arrays.copyOfRange(source, i * splitLength, (i + 1) * splitLength));
        }

        return result;
    }

    private List<byte[]> applyFormat(@Nonnull RecordIdFormat.TupleElement format, @Nonnull Tuple key) throws RecordCoreArgumentException {
        List<RecordIdFormat.FormatElement> formatElements = format.getChildren();
        if (key.size() != formatElements.size()) {
            throw new RecordCoreFormatException("Key tuple and format have different sizes, format cannot be applied")
                    .addLogInfo("tupleSize", key.size())
                    .addLogInfo("keySize", formatElements.size());
        }
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < formatElements.size(); i++) {
            RecordIdFormat.FormatElement formatElement = formatElements.get(i);
            Object keyElement = key.get(i);

            if (formatElement instanceof RecordIdFormat.TupleElement) {
                // recursive call
                result.addAll(applyFormat((RecordIdFormat.TupleElement)formatElement, verifyTuple(keyElement)));
            } else if (formatElement instanceof RecordIdFormat.FormatElementType) {
                // Recursion termination condition
                byte[] byteArray = applyFormat((RecordIdFormat.FormatElementType)formatElement, keyElement);
                if (byteArray != null) {
                    result.add(byteArray);
                }
            } else {
                // This should never happen
                throw new RecordCoreFormatException("Unknown element type")
                        .addLogInfo("type", formatElement.getClass().getName());
            }
        }
        return result;
    }

    private Tuple verifyTuple(final Object keyElement) {
        if (!(keyElement instanceof Tuple)) {
            throw new RecordCoreFormatException("Format mismatch: expected Tuple")
                    .addLogInfo("actualType", keyElement.getClass().getName());
        }
        return (Tuple)keyElement;
    }

    private byte[] applyFormat(final RecordIdFormat.FormatElementType formatElement, final Object tupleElement) {
        Object value = null;
        switch (formatElement) {
        case NONE:
            return null;

        case NULL:
            value = null;
            break;

        case INT32:
            if (!(tupleElement instanceof Integer)) {
                throw new RecordCoreFormatException("Format mismatch: Expected Integer")
                        .addLogInfo("actualType", tupleElement.getClass().getName());
            }
            value = tupleElement;
            break;

        case INT64:
            if (!(tupleElement instanceof Long)) {
                throw new RecordCoreFormatException("Format mismatch: Expected Long")
                        .addLogInfo("actualType", tupleElement.getClass().getName());
            }
            value = tupleElement;
            break;

        case INT32_OR_NULL:
            if ((tupleElement != null) && !(tupleElement instanceof Integer)) {
                throw new RecordCoreFormatException("Format mismatch: Expected Integer OR null")
                        .addLogInfo("actualType", tupleElement.getClass().getName());
            }
            value = tupleElement;
            break;

        case INT64_OR_NULL:
            if ((tupleElement != null) && !(tupleElement instanceof Long)) {
                throw new RecordCoreFormatException("Format mismatch: Expected Long OR null")
                        .addLogInfo("actualType", tupleElement.getClass().getName());
            }
            value = tupleElement;
            break;

        case STRING_16:
            if (!(tupleElement instanceof String)) {
                throw new RecordCoreFormatException("Format mismatch: Expected String")
                        .addLogInfo("actualType", tupleElement.getClass().getName());
            }
            if (((String)tupleElement).length() > 16) {
                throw new RecordCoreFormatException("Format mismatch: String too long")
                        .addLogInfo("actualLength", ((String)tupleElement).length());
            }
            value = tupleElement;
            break;

        case UUID_AS_STRING:
            if (!(tupleElement instanceof String)) {
                throw new RecordCoreFormatException("Format mismatch: Expected String")
                        .addLogInfo("actualType", tupleElement.getClass().getName());
            }
            try {
                value = UUID.fromString((String)tupleElement);
            } catch (Exception ex) {
                throw new RecordCoreFormatException("Format mismatch: Failed to parse UUID")
                        .addLogInfo("actualValue", tupleElement);
            }
            break;

        default:
            // this should not happen
            throw new RecordCoreFormatException("Format mismatch: unknown format")
                    .addLogInfo("format", formatElement);
        }

        int length = formatElement.getAllocatedSize();
        byte[] src = Tuple.from(value).pack();
        // Since we know for sure that length will be sufficient for the serialized value, (and will likely add padding), use it
        // for the length of the final byte array
        return Arrays.copyOfRange(src, 0, length);
    }

}
