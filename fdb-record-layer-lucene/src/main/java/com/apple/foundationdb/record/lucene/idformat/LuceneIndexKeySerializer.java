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
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.document.BinaryPoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class that serializes the Index primary keys according to a format.
 * When the {@link LuceneIndexMaintainer} writes an index entry, there are several ways
 * by which the index primary key can be written (stored, sortable, binarypoint, etc). This class generates the key values
 * that can be written to the document.
 * The key being serialized is the {@link Tuple} that represents the ID for the entry. The serialized format is a byte array
 * (e.g. it can be simply a {@code Tuple.pack()} in some cases, and can take other forms like {@link BinaryPoint}
 * for fast lookup).
 * This class uses a {@link RecordIdFormat} as a guide to the way that the serialized form is to be generated. The Format
 * contains instructions regarding the types and nesting hierarchy of the key so that the generated format is unique, consistent
 * and searchable.
 * Of note are the constraints of {@link org.apache.lucene.document.BinaryPoint}: All dimensions should be of the same length,
 * we cannot change the dimension length or the number of dimensions once an entry has been written (all entries should
 * have the same dimensions). As a consequence, this class allocates (and pads the byte arrays) the maximum size for each component
 * of the binary point. This leads to some waste, but is necessary in order to support the format.
 */
public class LuceneIndexKeySerializer {
    // The size of each of the dimensions. 9 bytes is enough for a Tuple-encoded INT64, so using this size would reduce
    // the overall waste of multiple dimensions of various sizes (INT64 is a common size to be used). We can't use 8
    // bytes since we need to support NULL values so have to add one byte to distinguish INT64 from NULL.
    public static final int BINARY_POINT_DIMENSION_SIZE = 9;
    public static final int MAX_STRING_16_LENGTH = 16;
    // Maximum, size of String16, when encoded. Allows for mostly ASCII characters when UTF-8 encoded, and fits in 3 dimensions
    public static final int MAX_STRING_16_ENCODED_LENGTH = 27;
    private static final String ACTUAL_TYPE = "actualType";

    @Nullable
    private final RecordIdFormat format;
    @Nonnull
    private final Tuple key;

    /**
     * Construct a new serializer using a format. The format parameter is optional. Absence of the format parameter will
     * cause failure when trying to use the {@link #asFormattedBinaryPoint} method.
     * @param format optional the key format
     * @param key the index primary key
     */
    public LuceneIndexKeySerializer(@Nullable RecordIdFormat format, @Nonnull final Tuple key) {
        this.key = key;
        this.format = format;
    }

    /**
     * Construct a new serializer using a String format. The format is parsed using {@link RecordIdFormatParser} (see that
     * class for a description of the syntax). The format parameter is optional. Absence of the format parameter will
     * cause failure when trying to use the {@link #asFormattedBinaryPoint} method.
     * @param formatString optional string representing the key format
     * @param key the index primary key
     * @return the created serializer
     */
    public static LuceneIndexKeySerializer fromStringFormat(@Nullable final String formatString, @Nonnull final Tuple key) {
        RecordIdFormat format = (formatString == null) ? null : RecordIdFormatParser.parse(formatString);
        return new LuceneIndexKeySerializer(format, key);
    }

    /**
     * Serialize the key as a single byte array (this will result in a {@code Tuple.pack()}).
     * @return the serialized key
     */
    public byte[] asPackedByteArray() {
        return key.pack();
    }

    /**
     * Serialize the key as a {@link BinaryPoint} without a format. This format SHOULD NOT be used to store in Lucene as
     * it is not guaranteed to be of fixed length. As the content grows, it may generate more dimensions.
     * @return the split (BinaryPoint) style serialized key
     */
    public byte[][] asPackedBinaryPoint() {
        // We might use a different dimension size here
        List<byte[]> splitBytes = split(key.pack(), BINARY_POINT_DIMENSION_SIZE);
        return splitBytes.toArray(new byte[splitBytes.size()][]);
    }

    /**
     * Serialize the key as a {@link BinaryPoint}. This method uses the given {@link RecordIdFormat} as a guide to ensure
     * the returned BinaryPoint is going to be of fixed length, and searchable (by separating the components to different
     * dimensions, thus allowing range searches).
     * @return the formatted serialized key
     * @throws RecordCoreFormatException in case there is no format or the format failed to parse or validate
     */
    public byte[][] asFormattedBinaryPoint() throws RecordCoreFormatException {
        if (format == null) {
            throw new RecordCoreFormatException("Missing format, cannot format to a BinaryPoint");
        }

        List<byte[]> formattedBytes = applyFormat(format.getElement(), key);
        List<byte[]> splitBytes = splitAll(formattedBytes);
        return splitBytes.toArray(new byte[splitBytes.size()][]);
    }

    /**
     * Split all the byte arrays to the dimension size.
     * @param byteArrayList the given list of arrays
     * @return the list of arrays, where arrays longer than the dimension size are split
     */
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

    /**
     * Split a single array to fit in a dimension size, padding if necessary. Shorter arrays are padded, longer ones are
     * split and then padded to the next dimension-sized array.
     * @param source the given array
     * @param splitLength the dimension length
     * @return the array that was split to fit in a number of dimension-sized chunks
     */
    // package protected for testing
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

    private Tuple verifyTuple(final Object keyElement) {
        if (!(keyElement instanceof Tuple)) {
            throw new RecordCoreFormatException("Format mismatch: expected Tuple")
                    .addLogInfo(ACTUAL_TYPE, keyElement.getClass().getName());
        }
        return (Tuple)keyElement;
    }

    /**
     * Apply a format to the key, returning the {@link BinaryPoint} compatible list of arrays. The format is verified against
     * the actual key, the maximum size of for the type of key elements is allocated and each element is separate to a
     * different dimension.
     * Note that there is no nesting indication in the flattened result - all elements from all tuples are in a single list.
     * This is OK since teh structure of the key is not allowed to be changed once the index is populated, and so we assume
     * that the only allowable variance is the value of each element.
     * @param format the key format
     * @param key the key tuple
     * @return the formatted key as a BinaryPoint
     * @throws RecordCoreArgumentException in case the format failed to verify or the key did nto match
     */
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
                // Skip if null (e.g. NONE element)
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

    @SuppressWarnings("java:S3776")
    @Nullable
    private byte[] applyFormat(final RecordIdFormat.FormatElementType formatElement, final Object tupleElement) {
        byte[] value;
        switch (formatElement) {
            case NONE:
                // Not serializing anything
                return null;

            case NULL:
                value = Tuple.from((Object)null).pack();
                break;

            case INT32:
                if (!(tupleElement instanceof Integer)) {
                    throw new RecordCoreFormatException("Format mismatch: Expected Integer")
                            .addLogInfo(ACTUAL_TYPE, tupleElement.getClass().getName());
                }
                value = Tuple.from(tupleElement).pack();
                break;

            case INT64:
                if (!(tupleElement instanceof Long)) {
                    throw new RecordCoreFormatException("Format mismatch: Expected Long")
                            .addLogInfo(ACTUAL_TYPE, tupleElement.getClass().getName());
                }
                value = Tuple.from(tupleElement).pack();
                break;

            case INT32_OR_NULL:
                if ((tupleElement != null) && !(tupleElement instanceof Integer)) {
                    throw new RecordCoreFormatException("Format mismatch: Expected Integer OR null")
                            .addLogInfo(ACTUAL_TYPE, tupleElement.getClass().getName());
                }
                value = Tuple.from(tupleElement).pack();
                break;

            case INT64_OR_NULL:
                if ((tupleElement != null) && !(tupleElement instanceof Long)) {
                    throw new RecordCoreFormatException("Format mismatch: Expected Long OR null")
                            .addLogInfo(ACTUAL_TYPE, tupleElement.getClass().getName());
                }
                value = Tuple.from(tupleElement).pack();
                break;

            case STRING_16:
                if (!(tupleElement instanceof String)) {
                    throw new RecordCoreFormatException("Format mismatch: Expected String")
                            .addLogInfo(ACTUAL_TYPE, tupleElement.getClass().getName());
                }
                if (((String)tupleElement).length() > MAX_STRING_16_LENGTH) {
                    throw new RecordCoreFormatException("Format mismatch: String too long")
                            .addLogInfo("allowedLength", MAX_STRING_16_LENGTH)
                            .addLogInfo("actualLength", ((String)tupleElement).length());
                }
                // Use String encoding here to save the prefix and suffix characters
                value = ((String)tupleElement).getBytes(StandardCharsets.UTF_8);
                if (value.length > MAX_STRING_16_ENCODED_LENGTH) {
                    throw new RecordCoreSizeException("Encoded string too long")
                            .addLogInfo("allowedLength", MAX_STRING_16_ENCODED_LENGTH)
                            .addLogInfo("actualLength", value.length);
                }
                break;

            case UUID_AS_STRING:
                if (!(tupleElement instanceof String)) {
                    throw new RecordCoreFormatException("Format mismatch: Expected String")
                            .addLogInfo(ACTUAL_TYPE, tupleElement.getClass().getName());
                }
                try {
                    value = Tuple.from(UUID.fromString((String)tupleElement)).pack();
                } catch (Exception ex) {
                    throw new RecordCoreFormatException("Format mismatch: Failed to parse UUID", ex)
                            .addLogInfo("actualValue", tupleElement);
                }
                break;

            default:
                // this should not happen
                throw new RecordCoreFormatException("Format mismatch: unknown format")
                        .addLogInfo("format", formatElement);
        }

        int length = formatElement.getAllocatedSize();
        // Since we know for sure that length will be sufficient for the serialized value, (and will likely add padding), use it
        // for the length of the final byte array
        return Arrays.copyOfRange(value, 0, length);
    }
}
