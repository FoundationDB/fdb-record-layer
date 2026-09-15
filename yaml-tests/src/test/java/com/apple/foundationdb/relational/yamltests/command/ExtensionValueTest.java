/*
 * ExtensionValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.yamltests.generated.extensiontests.ExtensionTestsProto;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the value conversions in {@link JsonExtensionMerger}, over the extensions of {@code extension_tests.proto}.
 * The metadata protos declare only message-valued extensions and none of their value messages is extendable, so a
 * proto2 file of our own is the only way to reach the other conversions or to nest one extension inside another.
 * <p>
 * These merge into a bare {@code FieldOptions} rather than a whole metadata document, which is all the conversions need.
 * </p>
 */
class ExtensionValueTest {
    private static final String EXT = "com.apple.foundationdb.relational.yamltests.generated.extensiontests.";

    @Test
    void scalarExtensionsAreConverted() throws Exception {
        final DescriptorProtos.FieldOptions options = mergeOptions(String.format("""
                {"%1$sa_bool": true,
                 "%1$san_int": -7,
                 "%1$sa_long": "-9223372036854775808",
                 "%1$sa_double": 1.5,
                 "%1$sa_string": "hello",
                 "%1$ssome_bytes": "q80+/w==",
                 "%1$sa_kind": "SECOND"}
                """, EXT));

        assertThat(options.getExtension(ExtensionTestsProto.aBool)).isTrue();
        assertThat(options.getExtension(ExtensionTestsProto.anInt)).isEqualTo(-7);
        assertThat(options.getExtension(ExtensionTestsProto.aLong)).isEqualTo(Long.MIN_VALUE);
        assertThat(options.getExtension(ExtensionTestsProto.aDouble)).isEqualTo(1.5);
        assertThat(options.getExtension(ExtensionTestsProto.aString)).isEqualTo("hello");
        assertThat(options.getExtension(ExtensionTestsProto.someBytes))
                .isEqualTo(ByteString.copyFrom(new byte[] {(byte) 0xab, (byte) 0xcd, 0x3e, (byte) 0xff}));
        assertThat(options.getExtension(ExtensionTestsProto.aKind)).isEqualTo(ExtensionTestsProto.Kind.SECOND);
    }

    /** The forms {@code JsonFormat} accepts but the printer does not produce. */
    @Test
    void alternativeSpellingsAreAccepted() throws Exception {
        final DescriptorProtos.FieldOptions options = mergeOptions(String.format("""
                {"%1$ssome_bytes": "q80-_w==",
                 "%1$sa_kind": 2}
                """, EXT));

        assertThat(options.getExtension(ExtensionTestsProto.someBytes))
                .isEqualTo(ByteString.copyFrom(new byte[] {(byte) 0xab, (byte) 0xcd, 0x3e, (byte) 0xff}));
        assertThat(options.getExtension(ExtensionTestsProto.aKind)).isEqualTo(ExtensionTestsProto.Kind.SECOND);
    }

    /**
     * The value of an extension is an ordinary message, so it may carry extensions of its own. This is the case the
     * metadata protos cannot express, and the one the recursion exists for.
     */
    @Test
    void anExtensionOfAnExtensionIsMerged() throws Exception {
        final DescriptorProtos.FieldOptions options = mergeOptions(String.format("""
                {"%1$scarrier": {"depth": 1, "%1$sinner_tag": "nested"}}
                """, EXT));

        final ExtensionTestsProto.Carrier carrier = options.getExtension(ExtensionTestsProto.carrier);
        assertThat(carrier.getDepth()).isEqualTo(1);
        assertThat(carrier.getExtension(ExtensionTestsProto.innerTag)).isEqualTo("nested");
    }

    /** Coercing any of these would land on a value indistinguishable from the field's default. */
    @Test
    void valuesOfTheWrongShapeAreRejected() {
        assertThatThrownBy(() -> mergeOptions(String.format("{\"%san_int\": 1.5}", EXT)))
                .hasMessageContaining("an_int")
                .hasMessageContaining("whole number");
        assertThatThrownBy(() -> mergeOptions(String.format("{\"%san_int\": 5000000000}", EXT)))
                .hasMessageContaining("an_int")
                .hasMessageContaining("32 bit");
        assertThatThrownBy(() -> mergeOptions(String.format("{\"%sa_bool\": \"1\"}", EXT)))
                .hasMessageContaining("a_bool")
                .hasMessageContaining("boolean");
        assertThatThrownBy(() -> mergeOptions(String.format("{\"%sa_kind\": \"THIRD\"}", EXT)))
                .hasMessageContaining("a_kind");
    }

    @Nonnull
    private static DescriptorProtos.FieldOptions mergeOptions(@Nonnull String optionsJson) throws Exception {
        final DescriptorProtos.FieldOptions.Builder builder = DescriptorProtos.FieldOptions.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(optionsJson, builder);
        JsonExtensionMerger.merge(builder, JsonParser.parseString(optionsJson).getAsJsonObject(),
                List.of(ExtensionTestsProto.class));
        return builder.build();
    }
}
