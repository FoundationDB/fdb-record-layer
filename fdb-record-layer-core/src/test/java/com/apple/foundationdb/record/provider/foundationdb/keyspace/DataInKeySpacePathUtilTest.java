/*
 * DataInKeySpacePathUtilTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersionTestUtils;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link DataInKeySpacePathUtil}.
 */
class DataInKeySpacePathUtilTest {

    private static final Tuple STORE_INFO_REMAINDER = Tuple.from(FDBRecordStoreKeyspace.STORE_INFO.id());

    private static final KeySpacePath DUMMY_PATH = new KeySpace(
            new KeySpaceDirectory("root", KeySpaceDirectory.KeyType.STRING, "test")
    ).path("root");

    @Test
    void checkMaxFormatVersion() {
        // This should only fail when a new format version is added. If this test fails, the developer should
        // validate that bumpIncarnationIfStoreInfo handles the new format version correctly, then update this
        // to the new value.
        assertEquals(FormatVersion.FULL_STORE_LOCK, FormatVersion.getMaximumSupportedVersion(),
                "The behavior of bumpIncarnationIfStoreInfo needs to be validated with the new format version");
    }

    @ParameterizedTest
    @EnumSource(FormatVersion.class)
    void bumpIncarnationForStoreInfoWithNonZeroIncarnation(FormatVersion formatVersion) throws InvalidProtocolBufferException {
        final int originalIncarnation = 5;
        final DataInKeySpacePath entry = storeInfoEntry(formatVersion, originalIncarnation);

        final DataInKeySpacePath result = DataInKeySpacePathUtil.bumpIncarnationIfStoreInfo(entry);

        if (formatVersion.isAtLeast(FormatVersion.INCARNATION)) {
            assertNotSame(entry, result);
            final RecordMetaDataProto.DataStoreInfo resultInfo =
                    RecordMetaDataProto.DataStoreInfo.parseFrom(result.getValue());
            assertEquals(originalIncarnation + 1, resultInfo.getIncarnation());
            assertEquals(entry.getPath(), result.getPath());
            assertEquals(entry.getRemainder(), result.getRemainder());
        } else {
            assertSame(entry, result);
        }
    }

    static Stream<Integer> nonPositiveIncarnations() {
        return Stream.of(0, -1, Integer.MIN_VALUE);
    }

    @ParameterizedTest
    @MethodSource("nonPositiveIncarnations")
    void doNotBumpIncarnationWhenNotPositive(int incarnation) throws InvalidProtocolBufferException {
        final DataInKeySpacePath entry = storeInfoEntry(FormatVersion.INCARNATION, incarnation);

        final DataInKeySpacePath result = DataInKeySpacePathUtil.bumpIncarnationIfStoreInfo(entry);

        assertSame(entry, result);
    }

    @Test
    void doNotBumpIncarnationWhenNotSet() throws InvalidProtocolBufferException {
        final RecordMetaDataProto.DataStoreInfo.Builder builder = RecordMetaDataProto.DataStoreInfo.newBuilder();
        FormatVersionTestUtils.addToStoreInfo(builder, FormatVersion.INCARNATION);
        final DataInKeySpacePath entry = new DataInKeySpacePath(DUMMY_PATH, STORE_INFO_REMAINDER,
                builder.build().toByteString());

        final DataInKeySpacePath result = DataInKeySpacePathUtil.bumpIncarnationIfStoreInfo(entry);

        // incarnation defaults to 0 when not set, so should not bump
        assertSame(entry, result);
    }

    static Stream<Tuple> remainders() {
        return Stream.of(
                STORE_INFO_REMAINDER,
                null,
                Tuple.from(FDBRecordStoreKeyspace.RECORD.id()),
                Tuple.from(FDBRecordStoreKeyspace.STORE_INFO.id(), FDBRecordStoreKeyspace.STORE_INFO.id()),
                Tuple.from(FDBRecordStoreKeyspace.STORE_INFO.id(), "extra"),
                Tuple.from(99L)
        );
    }

    @ParameterizedTest
    @MethodSource("remainders")
    void bumpIncarnationDependsOnRemainder(@Nullable Tuple remainder) throws InvalidProtocolBufferException {
        final int originalIncarnation = 5;
        final RecordMetaDataProto.DataStoreInfo storeInfo = buildStoreInfo(FormatVersion.INCARNATION, originalIncarnation);
        final DataInKeySpacePath entry = new DataInKeySpacePath(DUMMY_PATH, remainder,
                storeInfo.toByteString());

        final DataInKeySpacePath result = DataInKeySpacePathUtil.bumpIncarnationIfStoreInfo(entry);

        if (STORE_INFO_REMAINDER.equals(remainder)) {
            assertNotSame(entry, result);
            final RecordMetaDataProto.DataStoreInfo resultInfo =
                    RecordMetaDataProto.DataStoreInfo.parseFrom(result.getValue());
            assertEquals(originalIncarnation + 1, resultInfo.getIncarnation());
        } else {
            assertSame(entry, result);
        }
    }

    @Test
    void doNotBumpIncarnationWhenNoFormatVersion() throws InvalidProtocolBufferException {
        final RecordMetaDataProto.DataStoreInfo storeInfo = RecordMetaDataProto.DataStoreInfo.newBuilder()
                .setIncarnation(5)
                .build();
        final DataInKeySpacePath entry = new DataInKeySpacePath(DUMMY_PATH, STORE_INFO_REMAINDER,
                storeInfo.toByteString());

        final DataInKeySpacePath result = DataInKeySpacePathUtil.bumpIncarnationIfStoreInfo(entry);

        assertSame(entry, result);
    }

    @Test
    void preservesOtherFieldsWhenBumping() throws InvalidProtocolBufferException {
        final RecordMetaDataProto.DataStoreInfo.Builder builder = RecordMetaDataProto.DataStoreInfo.newBuilder()
                .setMetaDataversion(42)
                .setUserVersion(7)
                .setLastUpdateTime(123456789L)
                .setIncarnation(3);
        FormatVersionTestUtils.addToStoreInfo(builder, FormatVersion.INCARNATION);
        final DataInKeySpacePath entry = new DataInKeySpacePath(DUMMY_PATH, STORE_INFO_REMAINDER,
                builder.build().toByteString());

        final DataInKeySpacePath result = DataInKeySpacePathUtil.bumpIncarnationIfStoreInfo(entry);

        final RecordMetaDataProto.DataStoreInfo resultInfo =
                RecordMetaDataProto.DataStoreInfo.parseFrom(result.getValue());
        assertEquals(4, resultInfo.getIncarnation());
        assertEquals(42, resultInfo.getMetaDataversion());
        assertEquals(7, resultInfo.getUserVersion());
        assertEquals(123456789L, resultInfo.getLastUpdateTime());
    }

    // --- Helper methods ---

    private static DataInKeySpacePath storeInfoEntry(@Nonnull FormatVersion formatVersion, int incarnation) {
        final RecordMetaDataProto.DataStoreInfo storeInfo = buildStoreInfo(formatVersion, incarnation);
        return new DataInKeySpacePath(DUMMY_PATH, STORE_INFO_REMAINDER, storeInfo.toByteString());
    }

    private static RecordMetaDataProto.DataStoreInfo buildStoreInfo(@Nonnull FormatVersion formatVersion, int incarnation) {
        final RecordMetaDataProto.DataStoreInfo.Builder builder = RecordMetaDataProto.DataStoreInfo.newBuilder();
        FormatVersionTestUtils.addToStoreInfo(builder, formatVersion);
        if (incarnation != 0) {
            builder.setIncarnation(incarnation);
        }
        return builder.build();
    }
}
