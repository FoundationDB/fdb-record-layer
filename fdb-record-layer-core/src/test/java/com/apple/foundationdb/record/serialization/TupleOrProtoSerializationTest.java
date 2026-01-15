/*
 * TupleOrProtoSerializationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.serialization;

import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.BooleanSource;
import com.apple.test.RandomizedTestUtils;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test is designed to validate some edge cases around {@link Tuple} serialization.
 *
 * <p>
 * Protobuf messages are serialized by using a field encoding system. Each new Protobuf
 * message begins with a one (or more) byte length tag. The three least significant bits
 * specify the type of the message, and then the the next 4 most significant bits
 * specify the field number. If the field number exceeds 16, then the most significant bit
 * is set, and the field number encoding continues on the following byte using a 7 bit
 * little-Endian encoding scheme. More details on how this works, including the
 * meanings of the different wire types can be found <a href="https://protobuf.dev/programming-guides/encoding/">in
 * the Protobuf documentation</a>.
 * </p>
 *
 * <p>
 * {@link Tuple} values are encoded in similar way. Here, the first byte in any value is a
 * type code that indicates how to parse the next value. Unlike Protobuf, there is no
 * concept of field number, though the encodings need to have the property that
 * unsigned lexicographic comparison on the serialized arrays provides the same ordering
 * as lexicographic semantic comparison on the array elements.
 * </p>
 *
 * <p>
 * To get {@code byte} arrays that can be parsed in either encoding scheme, we can
 * take a closer look at the various {@link Tuple} codes and determine which ones
 * would lead to potential ambiguities:
 * </p>
 *
 * <ul>
 *     <li>
 *         <b>{@code \x00}, {@code \x01}, {@code \x02}, {@code \x05}</b>: These represent
 *         {@code null}, {@code byte[]}, {@code String}, and nested {@code Tuple} types.
 *         They all begin with five leading zeros, which in Protobuf messages, represents
 *         a field of zero. That is not a legal Protobuf field number, and so we'd expect parsing
 *         any such message to lead to an {@link com.google.protobuf.InvalidProtocolBufferException}.
 *     </li>
 *     <li>
 *         <b>{@code \x0b}-{@code \x1d}</b>: These are the codes for integer values, centered
 *         around {@code \x14} for zero. Note that the first 5 bits of this are always between
 *         1 and 3, so this means that the serialized bytes can represent one of three
 *         different fields in Protobuf. The final three bits also span all possible legal
 *         (and non-legal) wire types. So, for each integer length, the final three bits
 *         will set constraints on which integer values could be parsed as a Protobuf
 *         message.
 *     </li>
 *     <li>
 *         <b>{@code \x20}</b>: This is the code for {@code float}s in {@link Tuple}s.
 *         The last three bits are 0, which Protobuf would use to indicate that a variable
 *         length integer field should follow. The five leading bits indicate that this
 *         is field four of the Protobuf. The {@link Tuple}-encoding of a {@code float}
 *         gives us four bytes to play around with to construct a valid field.
 *     </li>
 *     <li>
 *         <b>{@code \x21}</b>: This is the code for {@code double}s in {@link Tuple}s.
 *         In Protobuf messages, the last three bits are 1, indicating that an eight
 *         byte value should follow. This is the same number of bytes as the {@link Tuple}
 *         representation of a {@code double}, so we just need to account for the way
 *         {@code Tuple}s represent data to re-interpret the same bytes.
 *     </li>
 *     <li>
 *         <b>{@code \x26} and {@code \x27}</b>: These are the two codes for Boolean
 *         values in {@link Tuple}s. Protobuf messages always begin with a byte whose
 *         last three significant bits form a code between 0 and 5. For these two types,
 *         those fields are not in a valid type code range, so it will not parse as a Protobuf.
 *     </li>
 *     <li>
 *         <b>{@code \x30}</b>: This is the code for {@link UUID}s. Those
 *         are then followed by 16 bytes representing the UUID data. The Protobuf
 *         wire type on this is zero, which represents a variable length integer.
 *         The first five bits represent field tag 6, so any message with a variable
 *         length integer type at field 6 could theoretically be parsed from a UUID
 *         as long as there is appropriate padding for the remaining bytes.
 *     </li>
 *     <li>
 *         <b>{@code \x33}</b>: This is the code for {@link com.apple.foundationdb.tuple.Versionstamp}s.
 *         Those are then followed by 12 bytes of data, representing a FDB commit version.
 *         The first five bits represent field tag 6, just as with {@link UUID}s. The wire
 *         type, however, represents "group start", a deprecated feature of Protobuf
 *     </li>
 * </ul>
 *
 * <p>
 * These tests generate various {@link Tuple}s with special constraints designed to construct
 * such ambiguities. The main purpose is to then let use these to construct adversarial test
 * cases in situations where we may have data that may be either a {@link Tuple} or a Protobuf,
 * and we need to consider how we might tell them apart.
 * </p>
 *
 * @see <a href="https://protobuf.dev/programming-guides/encoding/">Protobuf encoding documentation</a>
 * @see <a href="https://github.com/apple/foundationdb/blob/main/design/tuple.md">FDB Tuple encoding</a>
 */
class TupleOrProtoSerializationTest {

    /**
     * Seeds to use for the randomized tests. These are just arbitrary numbers that were generated
     * once. In nightlies or other environments where randomized tests are enabled, this will include
     * additional random seeds. The seed is always included in each test name so that failed tests
     * can be re-evaluated.
     *
     * @return a stream of random seeds
     */
    @Nonnull
    static Stream<Long> randomSeeds() {
        return RandomizedTestUtils.randomSeeds(
                -5554294865106447099L,
                4234641960355217945L,
                -2750950983922062072L,
                -6805941860257292370L,
                -8407697589029290290L,
                -6315140848982185231L,
                182329047951478150L,
                2406279511674747598L,
                -8652201628819025047L,
                -7729256453164133675L,
                2454789994347161950L,
                4532052477454022629L,
                -5817262037824271201L,
                5117799135627442365L,
                4997399118225104083L,
                3222247176693124539L,
                -3040542319775793473L,
                2955480913619106341L,
                -6914171067016184749L,
                -8507119203217978732L,
                4232702206759139462L
        );
    }

    private static long flipEndianness(long l) {
        // Flip the byte order of a long. Do this by shifting the least significant bytes up
        // and by shifting the most significant bytes down.
        return (l << 56) | ((l & 0xFF00L) << 40) | ((l & 0xFF0000L) << 24) | ((l & 0xFF000000L) << 8)
                | ((l >>> 8) & 0xFF000000L) | ((l >>> 24) & 0xFF0000L) | ((l >>> 40) & 0XFF00L) | (l >>> 56);
    }

    private static byte[] toLittleEndianBytes(long l) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)(l >>> (8 * i));
        }
        return bytes;
    }

    private static long toDoubleEncoding(double d) {
        // First, find the protobuf encoding for this double. Note that we do bit
        // arithmetic so that the unsigned byte order matches the semantic ordering.
        long l = Double.doubleToLongBits(d);
        if (l < 0) {
            // Negative numbers flip all the bits so that the sign bit is zero and the
            // rest of the bits now sort in their opposite order
            l ^= 0xFFFFFFFFFFFFFFFFL;
        } else {
            // Positive numbers flip only their sign bit so that values appear in their
            // expected order.
            l ^= 0x8000000000000000L;
        }
        // Then, flip endianness. Protobuf stores in little-Endian order whereas Tuple are in big-Endian
        return flipEndianness(l);
    }

    private static InvalidProtocolBufferException assertInvalidProtobuf(@Nonnull byte[] bytes, @Nonnull Descriptors.Descriptor descriptor) {
        return assertThrows(InvalidProtocolBufferException.class,
                () -> DynamicMessage.parseFrom(descriptor, bytes));
    }

    private static void assertInvalidProtobuf(@Nonnull Tuple t, Consumer<InvalidProtocolBufferException> assertion) {
        byte[] tupleBytes = t.pack();
        for (Descriptors.Descriptor d : TupleOrProtoSerializationProto.getDescriptor().getMessageTypes()) {
            InvalidProtocolBufferException err = assertInvalidProtobuf(tupleBytes, d);
            assertion.accept(err);
        }
    }

    private static void assertInvalidProtobufFieldNumber(@Nonnull Tuple t) {
        assertInvalidProtobuf(t, err -> assertThat(err.getMessage(), containsString("Protocol message contained an invalid tag (zero)")));
    }

    private static void assertInvalidProtobufWireType(@Nonnull Tuple t) {
        assertInvalidProtobuf(t, err -> assertThat(err.getMessage(), containsString("Protocol message tag had invalid wire type")));
    }

    private static void assertInvalidProtobufEndGroup(@Nonnull Tuple t) {
        assertInvalidProtobuf(t, err -> assertThat(err.getMessage(), containsString("Protocol message end-group tag did not match expected")));
    }

    private static Message assertValidProtobuf(@Nonnull byte[] bytes, @Nonnull Descriptors.Descriptor descriptor) {
        return assertDoesNotThrow(() -> DynamicMessage.parseFrom(descriptor, bytes));
    }

    private static void assertValidIfFieldIsNotZero(@Nonnull Tuple t) {
        byte[] tBytes = Arrays.copyOf(t.pack(), t.getPackedSize());
        assertEquals(0, tBytes[0] & 0xF8); // Field tag should be set to zero for this tuple's serialization
        tBytes[0] |= 0x10;
        assertValidProtobuf(tBytes, TupleOrProtoSerializationProto.AmbiguousDouble.getDescriptor());
    }

    private static void assertSameSerialization(@Nonnull Tuple t, @Nonnull Message m) {
        byte[] tupleBytes = t.pack();
        byte[] messageBytes = m.toByteArray();
        assertArrayEquals(tupleBytes, messageBytes, () -> "Tuple " + t + " should have same serialization as protobuf message of type " + m.getDescriptorForType().getName() + ": " + m);

        final Message parsedTuple = assertValidProtobuf(tupleBytes, m.getDescriptorForType());
        assertEquals(m, parsedTuple);

        final Tuple parsedMessage = Tuple.fromBytes(messageBytes);
        assertEquals(t, parsedMessage);
    }

    private static double randomDouble(@Nonnull Random r) {
        // Generate a random double. This uses a random long generator
        // rather than r.nextDouble() to explore more of the double space.
        // The r.nextDouble() method returns a value in  [0.0, 1.0), which
        // means we get limited mantissa values. (However, we also avoid
        // NaN values as those can sometimes be normalized to all the same
        // NaN value, and we don't care about that use case here.)
        double d;
        do {
            long l = r.nextLong();
            d = Double.longBitsToDouble(l);
        } while (Double.isNaN(d));
        return d;
    }

    /**
     * Return a random integer which requires a specified number of bytes to represent
     * a random variable length integer.
     *
     * @param r a source of random bytes that represent a variable length integer
     * @param variableByteLength number of bytes
     * @return an array of random bytes that would represent a single variable length integer
     */
    private static byte[] randomVarIntBytes(@Nonnull Random r, int variableByteLength) {
        byte[] ret = new byte[variableByteLength];
        for (int i = 0; i < ret.length - 1; i++) {
            // Not the final byte. Can be any value but must have a 1 in the msb
            ret[i] = (byte)(r.nextInt(0x80) | 0x80);
        }
        // The final byte is special. The msb must be 0, and one of the other bytes
        // must be set to 1.
        ret[ret.length - 1] = (byte)(r.nextInt(0x7F) + 1);
        return ret;
    }

    private static int numberOfIntBytes(int intCode) {
        return Math.abs(intCode - 0x14);
    }

    @Nonnull
    private static BigInteger randomIntWithCode(@Nonnull Random r, int intCode) {
        int numberOfBytes = numberOfIntBytes(intCode);
        if (numberOfBytes == 0) {
            return BigInteger.ZERO;
        }
        if (numberOfBytes == 9) {
            // Big integer. The value should have between 9 and 100 bytes.
            // The format allows for bigger, but we can get what we need with
            // just 100.
            numberOfBytes = r.nextInt(100 - 9) + 9;
        }
        byte[] bytes = new byte[numberOfBytes];
        r.nextBytes(bytes);
        if (intCode < 0x14) {
            bytes[0] = (byte) Math.max(-126, Math.min(-2, (-1 * Math.abs(bytes[0]))));
        } else {
            bytes[0] = (byte) Math.min(126, Math.max(1, Math.abs(bytes[0])));
        }
        final BigInteger b = new BigInteger(bytes);
        assertEquals(intCode, Tuple.from(b).pack()[0]);
        return b;
    }

    private static long asVarLong(byte[] varInt, int start) {
        long ret = 0;
        for (int i = start; i < varInt.length; i++) {
            long b = varInt[i];
            ret |= ((b & 0x7FL) << (7 * (i - start)));
            if ((b & 0x80L) == 0) {
                break;
            }
        }
        return ret;
    }

    private static long asVarLong(byte[] varInt) {
        return asVarLong(varInt, 0);
    }

    private static int asVarInt(byte[] varInt, int start) {
        return (int) asVarLong(varInt, start);
    }

    private static int asVarInt(byte[] varInt) {
        return asVarInt(varInt, 0);
    }

    private static long asSignedVarLong(byte[] varInt, int start) {
        long ret = asVarLong(varInt, start);
        if ((ret & 1) == 0) {
            // Even. This is a positive number
            ret >>>= 1;
        } else {
            // Odd. This is a negative number
            ret += 1;
            ret >>>= 1;
            ret *= -1;
        }
        return ret;
    }

    private static long asSignedVarLong(byte[] varInt) {
        return asSignedVarLong(varInt, 0);
    }

    private static int asSignedVarInt(byte[] varInt, int start) {
        return (int) asSignedVarLong(varInt, start);
    }

    private static int asSignedVarInt(byte[] varInt) {
        return asSignedVarInt(varInt, 0);
    }

    private static long asFixedLong(byte[] data, int start) {
        assertThat("to construct long, data must have at least start + 8 bytes", start + 8, lessThanOrEqualTo(data.length));
        long l = 0;
        for (int i = 0; i < 8; i++) {
            l |= ((0xff & (long) data[i + start])) << (8 * i);
        }
        return l;
    }

    private static int asFixedInt(byte[] data, int start) {
        assertThat("to construct long, data must have at least start + 4 bytes", start + 4, lessThanOrEqualTo(data.length));
        int x = 0;
        for (int i = 0; i < 4; i++) {
            x |= ((0xff & (int) data[i + start])) << (8 * i);
        }
        return x;
    }

    private static Number asFixedInteger(byte[] data, int start, int bytes) {
        if (bytes == 8) {
            return asFixedLong(data, start);
        } else if (bytes == 4) {
            return asFixedInt(data, start);
        } else {
            return fail("unable to parse integer of length " + bytes);
        }
    }

    private static float floatFromBytes(byte[] bytes) {
        assertEquals(4, bytes.length);
        int bits = 0;
        for (int i = 0; i < bytes.length; i++) {
            bits |= (((0xFF & bytes[i]) << (8 * (3 - i))));
        }
        if (bits < 0) {
            bits ^= 0x80000000;
        } else {
            bits ^= 0xFFFFFFFF;
        }
        return Float.intBitsToFloat(bits);
    }

    @Nonnull
    private static UUID uuidFromBytes(@Nonnull byte[] bytes) {
        assertEquals(16, bytes.length);
        long msb = 0;
        for (int i = 0; i < 8; i++) {
            msb |= ((long) (0xFF & bytes[i])) << (8 * (7 - i));
        }
        long lsb = 0;
        for (int i = 8; i < 16; i++) {
            lsb |= ((long) (0xFF & bytes[i])) << (8 * (15 - i));
        }
        return new UUID(msb, lsb);
    }

    /**
     * Assert that {@code null} does not parse if it's the first element of a {@link Tuple}.
     * Note that the code for {@code null} is {@code \x00}. As a Protobuf tag, if that means
     * anything, the wire type (the least three significant bits) is a {@code 0b000}, meaning
     * a variable length integer, and the field tag is zero. Zero is not a valid field tag, but
     * also go ahead and construct a case where the tag is followed by a valid variable integer
     * (in this case, the single byte {@code \x27} for {@code true}).
     */
    @Test
    void nullDoesNotParse() {
        Tuple t = Tuple.from((Object) null);
        assertInvalidProtobufFieldNumber(t);

        t = Tuple.from(null, true);
        assertInvalidProtobufFieldNumber(t);

        // Just to be sure, modify the first byte of t's serialization, and validate this would parse
        assertValidIfFieldIsNotZero(t);
    }

    /**
     * Assert that {@code byte[]} types do not parse if one is the first element of a {@link Tuple}.
     * Note that the code for {@code byte[]} is {@code \x01}. As a Protobuf tag, if that means
     * anything, the wire type (the least three significant bits) is {@code 0b001}, meaning
     * a fixed width 64-bit value, while the field tag is zero. Zero is not a valid field tag, but
     * also go ahead and construct a case where the tag is followed by seven bytes (plus the end tag
     * of the tuple)
     */
    @Test
    void bytesDoNotParse() {
        Tuple t = Tuple.from((Object) new byte[0]);
        assertInvalidProtobufFieldNumber(t);

        t = Tuple.from((Object) new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07});
        assertInvalidProtobufFieldNumber(t);

        // Just to be sure, modify the first byte of t's serialization so that the field number
        // is set, and then it should parse
        byte[] tBytes = Arrays.copyOf(t.pack(), t.getPackedSize());
        tBytes[0] |= 0x10;
        assertValidProtobuf(tBytes, TupleOrProtoSerializationProto.AmbiguousDouble.getDescriptor());
    }

    /**
     * Assert that {@link String} types do not parse if one is the first element of a {@link Tuple}.
     * Note that the code for {@link String} is {@code \x02}. As a Protobuf tag, if that means
     * anything, the wire type (the least three significant bits) is {@code 0b010}, meaning
     * length encoded bytes, while the field tag is zero. Zero is not a valid field tag, but
     * also go ahead and construct a case where the tag is followed by a number that describes
     * the rest of the length. This works for the empty string, but it also works for some more
     * crafty examples like one where the first character's ASCII code happens to be the
     * length of the {@link String}.
     */
    @Test
    void stringsDoNotParse() {
        Tuple t = Tuple.from("");
        assertInvalidProtobufFieldNumber(t);

        // Just to be sure, modify the first byte of t's serialization so that the field number
        // is set, and then it should parse. For empty strings, the first byte is the terminating
        // zero, which Protobuf would interpret as the length
        assertValidIfFieldIsNotZero(t);

        // Try a more complicated case. Here, the string's UTF-8 representation includes a valid
        // length that matches the remaining bytes in the Tuple. It still won't parse if the field
        // is not zero, but that's the only thing
        t = Tuple.from("&" + Strings.repeat("x", ((int) '&') - 1));
        assertInvalidProtobufFieldNumber(t);
        assertValidIfFieldIsNotZero(t);
    }

    /**
     * Assert that nested {@link Tuple} types do not parse if one is the first element of a {@link Tuple}.
     * Note that the code for {@link Tuple} is {@code \x05}. As a Protobuf tag, if that means
     * anything, the wire type (the least three significant bits) is {@code 0b101}, meaning
     * a fixed width 32-bit value, while the field tag is zero. Zero is not a valid field tag, but
     * also go ahead and construct a case where the nested Protobuf has a three byte serialization,
     * which would then mean that the rest of the Protobuf would parse as a valid 32-bit value.
     */
    @Test
    void nestedTuplesDoNotParse() {
        Tuple t = Tuple.from(TupleHelpers.EMPTY);
        assertInvalidProtobufFieldNumber(t);

        t = Tuple.from(Tuple.from(null, false));
        assertInvalidProtobufFieldNumber(t);
        assertValidIfFieldIsNotZero(t);
    }

    /**
     * Assert a {@link Tuple}-encoded Boolean value will not parse. This is because
     * the two {@link Tuple} codes for {@code false} and {@code true} are {@code \x26}
     * and {@code \x27}, and they have wire types 6 and 7 respectively, which are not
     * valid wire types (as those must be in the range 0-5).
     *
     * @param booleanValue the value to encode
     */
    @ParameterizedTest(name = "booleansDoNotParse[value={0}]")
    @BooleanSource
    void booleansDoNotParse(boolean booleanValue) {
        Tuple t = Tuple.from(booleanValue);
        assertInvalidProtobufWireType(t);
    }

    /**
     * Check that any double tuple can be misinterpreted as a {@code fixed64}.
     *
     * <p>
     * The {@link Tuple} encoding for a {@code double} is the byte code
     * {@code 0x21} followed by 8 bytes representing the double's IEEE 754
     * representation with some correction to make the sign bit correct.
     * In a Protobuf message, the leading byte code of {@code 0x21}, or in bits,
     * {@code 0b0010 0001}, represents setting field {@code 0b0100} (the four
     * most significant bits after the MSB) with type code {@code 0b001}
     * (the least significant bits). As doubles are also 8 bytes, that means
     * that any {@code Tuple} consisting of a single double can be
     * parsed as a Protobuf message with a single 64-bit number set to
     * field 4.
     * </p>
     *
     * @param randomSeed seed to used to generate random values
     */
    @ParameterizedTest(name = "anyDoubleIsFixed64")
    @MethodSource("randomSeeds")
    void anyDoubleIsFixed64(long randomSeed) {
        final Random r = new Random(randomSeed);
        double d = randomDouble(r);
        assertSameSerialization(
                Tuple.from(d),
                TupleOrProtoSerializationProto.AmbiguousFixed64.newBuilder()
                        .setF(toDoubleEncoding(d))
                        .build());
    }

    /**
     * Check that any double can be misinterpreted as an {@code sfixed64}.
     *
     * @param randomSeed seed to used to generate random values
     * @see #anyDoubleIsFixed64(long)
     */
    @ParameterizedTest(name = "anyDoubleIsFixed64[seed={0}]")
    @MethodSource("randomSeeds")
    void anyDoubleIsSFixed64(long randomSeed) {
        final Random r = new Random(randomSeed);
        double d = randomDouble(r);
        assertSameSerialization(
                Tuple.from(d),
                TupleOrProtoSerializationProto.AmbiguousSFixed64.newBuilder()
                        .setS(toDoubleEncoding(d))
                        .build());
    }

    /**
     * Check that any double can be misinterpreted as a different double.
     *
     * @param randomSeed seed to used to generate random values
     * @see #anyDoubleIsFixed64(long)
     */
    @ParameterizedTest(name = "anyDoubleIsDouble[seed={0}]")
    @MethodSource("randomSeeds")
    void anyDoubleIsDouble(long randomSeed) {
        final Random r = new Random(randomSeed);
        double d = randomDouble(r);
        assertSameSerialization(
                Tuple.from(d),
                TupleOrProtoSerializationProto.AmbiguousDouble.newBuilder()
                        .setD(Double.longBitsToDouble(toDoubleEncoding(d)))
                        .build());
    }

    /**
     * Test parsing floats as variable length integers. This generates a single
     * variable length encoded integer that takes up four bytes, and then it
     * constructs both a {@link Tuple} and a Protobuf message that wrap those
     * four variable-length encoded bytes.
     *
     * @param randomSeed seed to used to generate random values
     */
    @ParameterizedTest(name = "floatAsVarInt[seed={0}]")
    @MethodSource("randomSeeds")
    void floatAsVarInt(long randomSeed) {
        final Random r = new Random(randomSeed);
        byte[] varIntBytes = randomVarIntBytes(r, 4);
        Tuple t = Tuple.from(floatFromBytes(varIntBytes));

        double choice = r.nextDouble();
        Message m;
        if (choice < 0.25) {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsInt64.newBuilder()
                    .setX(asVarLong(varIntBytes))
                    .build();
        } else if (choice < 0.5) {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsSInt64.newBuilder()
                    .setX(asSignedVarLong(varIntBytes))
                    .build();
        } else if (choice < 0.75) {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsInt32.newBuilder()
                    .setX(asVarInt(varIntBytes))
                    .build();
        } else {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsSInt32.newBuilder()
                    .setX(asSignedVarInt(varIntBytes))
                    .build();
        }

        assertSameSerialization(t, m);
    }

    /**
     * Test parsing floats as two variable length integers. This generates a single
     * two variable length encoded integers, one that is one byte and another that is
     * two bytes. It then assigns them to fields in a Protobuf, and concatenates them
     * so that they represent some {@code float}. It then constructs a Protobuf with the
     * same serialization, and asserts that they match.
     *
     * @param randomSeed seed to used to generate random values
     */
    @ParameterizedTest(name = "floatAsTwoVarInts[seed={0}]")
    @MethodSource("randomSeeds")
    void floatAsTwoVarInts(long randomSeed) {
        final Random r = new Random(randomSeed);
        boolean biggerX = r.nextBoolean();

        byte[] xBytes = randomVarIntBytes(r, biggerX ? 2 : 1);
        byte[] code5 = new byte[]{ 0x28 }; // Represents field 5
        byte[] yBytes = randomVarIntBytes(r, biggerX ? 1 : 2);
        byte[] floatBytes = ByteArrayUtil.join(xBytes, code5, yBytes);
        Tuple t = Tuple.from(floatFromBytes(floatBytes));

        double choice = r.nextDouble();
        Message m;
        if (choice < 0.25) {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsInt64.newBuilder()
                    .setX(asVarLong(xBytes))
                    .setY(asVarLong(yBytes))
                    .build();
        } else if (choice < 0.5) {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsSInt64.newBuilder()
                    .setX(asSignedVarLong(xBytes))
                    .setY(asSignedVarLong(yBytes))
                    .build();
        } else if (choice < 0.75) {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsInt32.newBuilder()
                    .setX(asVarInt(xBytes))
                    .setY(asVarInt(yBytes))
                    .build();
        } else {
            m = TupleOrProtoSerializationProto.AmbiguousFloatAsSInt32.newBuilder()
                    .setX(asSignedVarInt(xBytes))
                    .setY(asSignedVarInt(yBytes))
                    .build();
        }

        assertSameSerialization(t, m);
    }

    /**
     * Validate that a Protobuf can encode a Boolean value in a way that
     * is the same as a {@link Tuple} float. The Boolean itself is always
     * two bytes, and so to get to a four-byte float, it needs an additional
     * bit of data, in this case represented with a packed array of Booleans.
     *
     * @param boolValue the Boolean value to encode
     */
    @ParameterizedTest(name = "floatAsBool[boolValue={0}")
    @BooleanSource
    void floatAsBool(boolean boolValue) {
        TupleOrProtoSerializationProto.AmbiguousFloatAsBool m = TupleOrProtoSerializationProto.AmbiguousFloatAsBool.newBuilder()
                .setX(boolValue)
                .addR(boolValue)
                .build();
        byte booleanByte = boolValue ? (byte) 0x01 : (byte) 0x00;
        byte[] floatBytes = new byte[]{
                booleanByte,
                0x2A, // Represents field 5 with a variable length coded value (for packed repeated fields)
                0x01, // Size 1
                booleanByte
        };
        Tuple t = Tuple.from(floatFromBytes(floatBytes));

        assertSameSerialization(t, m);
    }

    /**
     * Validate that a {@link Tuple} can serialize a {@link UUID} in a
     * way that Protobuf would interpret it as two integer values.
     *
     * @param randomSeed seed to used to generate random values
     */
    @ParameterizedTest(name = "uuidAsTwoLongs[seed={0}]")
    @MethodSource("randomSeeds")
    void uuidAsTwoLongs(long randomSeed) {
        final Random r = new Random(randomSeed);
        boolean aIsBigger = r.nextBoolean();
        byte[] aBytes = randomVarIntBytes(r, aIsBigger ? 8 : 7);
        byte[] bTag = new byte[]{ 0x38 };
        byte[] bBytes = randomVarIntBytes(r, aIsBigger ? 7 : 8);

        TupleOrProtoSerializationProto.AmbiguousUuidAsInt m = TupleOrProtoSerializationProto.AmbiguousUuidAsInt.newBuilder()
                .setA(asVarLong(aBytes))
                .setB(asVarLong(bBytes))
                .build();

        byte[] uuidBytes = ByteArrayUtil.join(aBytes, bTag, bBytes);
        UUID uuid = uuidFromBytes(uuidBytes);
        assertSameSerialization(Tuple.from(uuid), m);
    }

    /**
     * Validate that a {@link Tuple} can serialize a {@link UUID} in a
     * way that Protobuf would interpret it as three integer values.
     * The serialized size for all three need to fit in the serialized
     * form for the {@link UUID}, but that's possible due to their
     * variable length encoding.
     *
     * @param randomSeed seed to used to generate random values
     */
    @ParameterizedTest(name = "uuidAsThreeLongs[seed={0}]")
    @MethodSource("randomSeeds")
    void uuidAsThreeLongs(long randomSeed) {
        final Random r = new Random(randomSeed);
        // a and b should each be between 1 and 5 bytes, with a total size of 7 with bTag
        byte[] aBytes = randomVarIntBytes(r, r.nextInt(5) + 1);
        byte[] bTag = new byte[]{ 0x38 };
        byte[] bBytes = randomVarIntBytes(r, 6 - aBytes.length);

        // c provides another 9 bytes
        byte[] cTag = new byte[]{ 0x41 };
        long c = r.nextLong();
        byte[] cBytes = toLittleEndianBytes(c);

        TupleOrProtoSerializationProto.AmbiguousUuidAsInt m = TupleOrProtoSerializationProto.AmbiguousUuidAsInt.newBuilder()
                .setA(asVarLong(aBytes))
                .setB(asVarLong(bBytes))
                .setC(c)
                .build();

        byte[] uuidBytes = ByteArrayUtil.join(aBytes, bTag, bBytes, cTag, cBytes);
        UUID uuid = uuidFromBytes(uuidBytes);
        assertSameSerialization(Tuple.from(uuid), m);
    }

    /**
     * Test that {@link Versionstamp}s can be parsed as messages. The type code
     * for versionstamps ({@code \x33}) necessitates using a deprecated Protobuf
     * type code. It's an older code, but it checks out.
     *
     * @param randomSeed seed to used to generate random values
     */
    @ParameterizedTest(name = "versionstampAsGroup[seed={0}]")
    @MethodSource("randomSeeds")
    void versionstampAsGroup(long randomSeed) {
        final Random r = new Random(randomSeed);
        byte[] randomBytes = new byte[9];
        r.nextBytes(randomBytes);

        final Message m = TupleOrProtoSerializationProto.AmbiguousVersionstampAsGroup.newBuilder()
                .setG(TupleOrProtoSerializationProto.AmbiguousVersionstampAsGroup.G.newBuilder()
                        .setX(ZeroCopyByteString.wrap(randomBytes))
                )
                .build();

        byte[] xTagAndLength = new byte[]{0x0A, 0x09}; // Add tag for nested x field in group
        byte[] gFooter = new byte[]{0x34}; // Add footer to close out the group
        byte[] versionBytes = ByteArrayUtil.join(xTagAndLength, randomBytes, gFooter);
        Versionstamp v = Versionstamp.fromBytes(versionBytes);

        assertSameSerialization(Tuple.from(v), m);
    }

    private int fieldNumberForIntCode(int code) {
        return code >> 3;
    }

    @Nonnull
    private NonnullPair<Tuple, Message> createAmbiguousIntAsLengthEncoded(@Nonnull Random r, int intCode) {
        // For this field, we set the length to the number of bytes after the first two. This
        // modifies the most significant byte of the Tuple value, and then it tells the Protobuf
        // parser to treat all of the rest of the data as a bytes field. Note that if we
        // had exactly 9 returned by the number of int bytes, then that would indicate that we
        // have a Tuple which sets its size using the same byte as Protobuf. We actually could
        // deal with that for some numbers, but we thankfully don't have a code that does that
        int numberOfIntBytes = numberOfIntBytes(intCode);
        assertThat(numberOfIntBytes, lessThanOrEqualTo(8));

        // Construct a random integer in the right range.
        final byte[] serialized = Tuple.from(randomIntWithCode(r, intCode)).pack();
        int lengthToEncode = serialized.length - 2; // subtract two for the size
        assertThat(lengthToEncode, greaterThanOrEqualTo(0));
        assertThat(lengthToEncode, lessThanOrEqualTo(127));
        serialized[1] = (byte) lengthToEncode; // Set the byte to the length
        final Tuple tFinal = Tuple.fromBytes(serialized);

        // Construct a message that sets a single bytes field to the tail of the serialized integer
        final Message.Builder builder = TupleOrProtoSerializationProto.AmbiguousIntAsBytes.newBuilder();
        final Descriptors.FieldDescriptor fieldDescriptor = TupleOrProtoSerializationProto.AmbiguousIntAsBytes.getDescriptor()
                .findFieldByNumber(fieldNumberForIntCode(intCode));
        builder.setField(fieldDescriptor, ByteString.copyFrom(serialized, 2, lengthToEncode));

        return NonnullPair.of(tFinal, builder.build());
    }

    @Nonnull
    private NonnullPair<Tuple, Message> createAmbiguousIntAsGroup(@Nonnull Random r, int intCode) {
        // The way a group is encoded, we set one byte to the opening tag and then another byte
        // to a closing tag, and the intermediate bytes represent the group's contents as a nested
        // message. In each of these cases, we construct a nested group where the interior data
        // is random, and the rest of the integer's values are set to form a valid group
        final int fieldNumber = fieldNumberForIntCode(intCode);
        final byte endCode = (byte)(fieldNumber << 3 | 0x04);
        int numberOfIntBytes = numberOfIntBytes(intCode);
        final Tuple t;
        final ByteString x;
        if (numberOfIntBytes < 9) {
            final byte[] serialized = Tuple.from(randomIntWithCode(r, intCode)).pack();
            if (serialized.length > 2) {
                serialized[1] = 0x0A; // field 1, length encoded
                serialized[2] = (byte)(serialized.length - 4); // Length is remainder of integer
                x = ByteString.copyFrom(serialized, 3, serialized.length - 4);
            } else {
                // We have just enough data for the beginning tag and the end tag. Do not
                // set any fields on the group
                x = null;
            }
            // Final byte needs to close out the group
            serialized[serialized.length - 1] = endCode;
            t = Tuple.fromBytes(serialized);
        } else {
            // The first byte after the type code in Tuple land represents the number of bytes
            // in the big integer. It needs to be 0x0A to represent setting field 1 of the
            // group to a length encoded field (in this case, a byte array with most of the contents
            // of the integer). If we're positive, a byte of 0x0A would represent a 10 byte integer.
            // If we're negative, we need to subtract the value from 255 to get the right ordering
            int bigIntSize = intCode > 0x14 ? 0x0A : (255 - 0x0A);
            byte[] bigIntBytes = new byte[bigIntSize];
            r.nextBytes(bigIntBytes);
            if (intCode > 0x14) {
                // Positive value. In Protobuf, the first byte represents the length, and then
                // the last bytes will be the end code for the group. All the data in the middle,
                // insert into the bytes field x.
                bigIntBytes[0] = (byte)(bigIntSize - 2);
                bigIntBytes[bigIntBytes.length - 1] = endCode;
                x = ByteString.copyFrom(bigIntBytes, 1, bigIntSize - 2);
            } else {
                // Negative value. The most significant byte needs to be negative for the BigInteger
                // to get the right type code. Luckily, to encode the length of the underlying array in
                // Protobuf, we need two bytes, the first of which must have a negative sign bit. Set those
                // first two bits of the integer to the var-int encoded size of the inner array, and then
                // set the final byte to the endCode + 1 (the +1 necessary as we will record this in one's
                // complement). All the intermediate bytes will become the byte array x
                int remainingLength = bigIntSize - 3;
                bigIntBytes[0] = (byte)(remainingLength & 0x7F | 0x80);
                bigIntBytes[1] = (byte)(remainingLength >>> 7);
                bigIntBytes[bigIntBytes.length - 1] = (byte) (endCode + 1);
                x = ByteString.copyFrom(bigIntBytes, 2, remainingLength);
            }
            t = Tuple.from(new BigInteger(bigIntBytes));
        }

        final Message m;
        if (fieldNumber == 1) {
            TupleOrProtoSerializationProto.AmbiguousIntAsGroup.A.Builder aBuilder = TupleOrProtoSerializationProto.AmbiguousIntAsGroup.A.newBuilder();
            if (x != null) {
                aBuilder.setX(x);
            }
            m = TupleOrProtoSerializationProto.AmbiguousIntAsGroup.newBuilder()
                    .setA(aBuilder)
                    .build();
        } else if (fieldNumber == 2) {
            TupleOrProtoSerializationProto.AmbiguousIntAsGroup.B.Builder bBuilder = TupleOrProtoSerializationProto.AmbiguousIntAsGroup.B.newBuilder();
            if (x != null) {
                bBuilder.setX(x);
            }
            m = TupleOrProtoSerializationProto.AmbiguousIntAsGroup.newBuilder()
                    .setB(bBuilder)
                    .build();
        } else if (fieldNumber == 3) {
            TupleOrProtoSerializationProto.AmbiguousIntAsGroup.C.Builder cBuilder = TupleOrProtoSerializationProto.AmbiguousIntAsGroup.C.newBuilder();
            if (x != null) {
                cBuilder.setX(x);
            }
            m = TupleOrProtoSerializationProto.AmbiguousIntAsGroup.newBuilder()
                    .setC(cBuilder)
                    .build();
        } else {
            m = fail("Unknown field number");
        }
        return NonnullPair.of(t, m);
    }

    @Nonnull
    private NonnullPair<Tuple, Message> createAmbiguousIntAsVarInt(@Nonnull Random r, int intCode) {
        final byte[] serialized = Tuple.from(randomIntWithCode(r, intCode)).pack();
        long varIntValue;
        int tailStart;
        if (numberOfIntBytes(intCode) < 9) {
            // For all int codes less than 9, the byte after the type code can be arbitrarily
            // changed, as long as it stays within (0, 0xff), so do that to make it a variable
            // length integer. So set the first byte's msb to 0 and then the var int value is
            // just the first byte
            serialized[1] &= 0x7f;
            if (serialized[1] == 0) {
                serialized[1] = 1;
            }

            // The first byte after the type code would already get parsed as a single var-int
            varIntValue = asVarLong(serialized, 1);
            tailStart = 2;
        } else if ((serialized[1] & 0x80) == 0x00) {
            // The first byte in the Tuple encoding represents a number of bytes. We don't need to
            // modify it as it will be parsed as a var int
            varIntValue = asVarLong(serialized, 1);
            tailStart = 2;
        } else {
            // The first byte here represents a length, and we don't want to modify it. Modify
            // the first byte after the length, keeping it in the range 0x01-0x7f.
            serialized[2] &= 0x7f;
            if (serialized[2] == 0) {
                serialized[2] = 1;
            }
            varIntValue = asVarLong(serialized, 1);
            tailStart = 3;
        }

        // Set the data within the integer so that the rest of the Tuple-encoded integer is
        // parsed by Protobuf as a byte array
        int lengthRemaining = serialized.length - tailStart - 2;
        assertThat(lengthRemaining, both(greaterThanOrEqualTo(0)).and(lessThan(127)));
        serialized[tailStart] = 0x22; // Field 4, wire type "length delimited"
        serialized[tailStart + 1] = (byte) lengthRemaining;

        final Tuple t = Tuple.fromBytes(serialized);

        final Descriptors.FieldDescriptor fieldDescriptor = TupleOrProtoSerializationProto.AmbiguousIntAsVarInt.getDescriptor()
                .findFieldByNumber(fieldNumberForIntCode(intCode));
        return NonnullPair.of(t,
                TupleOrProtoSerializationProto.AmbiguousIntAsVarInt.newBuilder()
                        .setField(fieldDescriptor, varIntValue)
                        .setX(ByteString.copyFrom(serialized, tailStart + 2, lengthRemaining))
                        .build());

    }

    @Nonnull
    private NonnullPair<Tuple, Message> createAmbiguousIntAsFixedValue(Random r, Descriptors.Descriptor descriptor, int intCode, int byteCount) {
        final Message.Builder builder = DynamicMessage.newBuilder(descriptor);
        final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumberForIntCode(intCode));
        int numberOfIntBytes = numberOfIntBytes(intCode);
        final Tuple tFinal;
        if (numberOfIntBytes <= byteCount) {
            // Need to construct a random integer, and then append it with additional data
            final BigInteger value = randomIntWithCode(r, intCode);
            Tuple t = Tuple.from(value);
            if (numberOfIntBytes == byteCount - 1) {
                // Need one more byte
                t = t.add(r.nextBoolean());
            } else if (numberOfIntBytes < byteCount - 1) {
                // Need at least two more bytes
                byte[] arr = new byte[byteCount - 2 - numberOfIntBytes];
                r.nextBytes(arr);
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i] == 0x00) {
                        arr[i]++;
                    }
                }
                t = t.add(arr);
            }
            tFinal = t;
            byte[] serialized = tFinal.pack();
            assertEquals(serialized[0], intCode, () -> "serialized tuple " + tFinal + " should have int code " + intCode);
            assertEquals(byteCount + 1, serialized.length);

            builder.setField(fieldDescriptor, asFixedInteger(serialized, 1, byteCount));
        } else if (numberOfIntBytes < 9) {
            // The size of the integer is fixed to the number of bytes set by the
            // type code.
            final BigInteger bigInteger = randomIntWithCode(r, intCode);
            final Tuple t = Tuple.from(bigInteger);
            byte[] serialized = t.pack();
            assertEquals(numberOfIntBytes + 1, serialized.length);
            assertThat(serialized.length, greaterThan(byteCount + 1));

            // In Protobuf, the first byte represents the tag for the field, and so the
            // rest of the bytes must be used for some kind of padding.
            if (serialized.length == byteCount + 2) {
                // The first byteCount bytes of the integer will be parsed as protobuf
                // value. The final byte is then parsed as the tag of the next value.
                // This will not fit, so return now. If we were to hit this case, we
                // could extend the Tuple to include extra data, but we don't actually have
                // a type code that hits this branch
                return fail("cannot fit integer into the data provided");
            } else {
                // The first byteCount + 1 bytes from the Tuple have been used to represent
                // a value. Treat the rest as binary data by setting field byteCount + 1
                // to the wire tag for the bytes field 4, and then field byteCount + 2 to the
                // remaining length. Copy the remainder into a byte string, and then that
                // will form the basis of the Protobuf message field
                serialized[byteCount + 1] = 0x22; // tag for field 4, type "length delimited"
                int remainingLength = serialized.length - byteCount - 3;
                assertThat(remainingLength, lessThanOrEqualTo(127));
                serialized[byteCount + 2] = (byte) remainingLength;

                tFinal = Tuple.fromBytes(serialized);
                builder.setField(fieldDescriptor, asFixedInteger(serialized, 1, byteCount));
                builder.setField(descriptor.findFieldByNumber(4), ByteString.copyFrom(serialized, byteCount + 3, remainingLength));
            }
        } else {
            // When the number of bytes is this big, it means that we have a big integer.
            // The first byte after the integer type code is the number of bytes, or the
            // 1's complement for negative numbers. Pick a size of no more than 100 bytes
            final BigInteger bigInteger = randomIntWithCode(r, intCode);
            final Tuple t = Tuple.from(bigInteger);
            byte[] serialized = t.pack();
            assertThat(serialized.length, greaterThanOrEqualTo(byteCount + 2));

            // We now have a Tuple with at least 11 bytes: the type code, the size byte,
            // and then at least 9 integer bytes. In Protobuf, the first byte will be the
            // tag, then the next 8 bytes a fixed integer value. We just need to set the final
            // bytes so that they appropriately pad out. Set up field 4 to represent
            // a byte array containing the rest of the data.
            serialized[byteCount + 1] = 0x22; // tag for field 4, type "length delimited"
            int remainingLength = serialized.length - byteCount - 3;
            assertThat(remainingLength, lessThan(127));
            serialized[byteCount + 2] = (byte) remainingLength;

            tFinal = Tuple.fromBytes(serialized);
            builder.setField(fieldDescriptor, asFixedInteger(serialized, 1, byteCount));
            builder.setField(descriptor.findFieldByNumber(4), ByteString.copyFrom(serialized, byteCount + 3, remainingLength));
        }

        return NonnullPair.of(tFinal, builder.build());
    }

    /**
     * Generate arguments for {@link #integerWithCode(int, long)}. This returns a {@link Tuple}
     * integer code as well as a random seed. The other test will then create a random tuple
     * that uses that integer code.
     *
     * @return a valid {@link Tuple} integer code and a seed
     */
    @Nonnull
    static Stream<Arguments> integerWithCode() {
        return IntStream.range(0x0b, 0x1e)
                .boxed()
                .flatMap(code -> randomSeeds().map(seed -> Arguments.of(code, seed)));
    }

    /**
     * Test cases involving integers with the given {@link Tuple}-layer type code.
     * Each legnth of integer effectively has its own type code. The last three
     * bits of the code tell us the Protobuf wire type, whereas the first five bits
     * tell us the field. Based on the wire type and field, we will generate data
     * where the {@link Tuple} code can be interpreted as a sensible Protobuf tag.
     *
     * @param code the {@link Tuple}-layer type code to test
     * @param randomSeed seed to use to generate random values
     */
    @ParameterizedTest(name = "integerWithCode[code={0}, seed={1}]")
    @MethodSource
    void integerWithCode(int code, long randomSeed) {
        final Random r = new Random(randomSeed);

        int wireType = code & 0x07;
        NonnullPair<Tuple, Message> data = null;
        switch (wireType) {
            case 0: // Variable length integer
                data = createAmbiguousIntAsVarInt(r, code);
                break;
            case 1: // Fixed 64-bit value
                data = createAmbiguousIntAsFixedValue(r, TupleOrProtoSerializationProto.AmbiguousIntAsFixed64.getDescriptor(), code, 8);
                break;
            case 2: // Length encoded value
                data = createAmbiguousIntAsLengthEncoded(r, code);
                break;
            case 3: // Start of group
                data = createAmbiguousIntAsGroup(r, code);
                break;
            case 4: // End of group
                // This wire type can never appear at the start of a message, so we always get an error
                final Tuple tWithEndCode = Tuple.from(randomIntWithCode(r, code));
                assertInvalidProtobufEndGroup(tWithEndCode);
                break;
            case 5: // Fixed 32-bit value
                data = createAmbiguousIntAsFixedValue(r, TupleOrProtoSerializationProto.AmbiguousIntAsFixed32.getDescriptor(), code, 4);
                break;
            default: // Invalid wire type
                final Tuple tWithInvalidWireType = Tuple.from(randomIntWithCode(r, code));
                assertInvalidProtobufWireType(tWithInvalidWireType);
                break;
        }
        if (data == null) {
            return;
        }
        assertSameSerialization(data.getLeft(), data.getRight());
        assertEquals(code, data.getLeft().pack()[0]);
    }
}
