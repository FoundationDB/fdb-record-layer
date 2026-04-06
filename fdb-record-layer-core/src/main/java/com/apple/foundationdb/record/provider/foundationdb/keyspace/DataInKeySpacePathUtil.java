/*
 * DataInKeySpacePathUtil.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;

/**
 * Utility methods for transforming {@link DataInKeySpacePath} entries during export/import operations.
 */
@API(API.Status.EXPERIMENTAL)
public class DataInKeySpacePathUtil {

    private DataInKeySpacePathUtil() {
    }

    /**
     * If the given {@link DataInKeySpacePath} represents a store info record (i.e., its remainder key matches
     * {@link FDBRecordStoreKeyspace#STORE_INFO}), and the store's format version supports incarnation, and the
     * current incarnation is not zero, then return a new {@link DataInKeySpacePath} with the incarnation incremented
     * by one. Otherwise, return the original entry unchanged.
     *
     * @param dataInKeySpacePath the data entry to potentially transform
     * @return the entry with bumped incarnation, or the original entry if no bump is needed
     * @throws InvalidProtocolBufferException if the value of a store info entry cannot be parsed as a
     *         {@link RecordMetaDataProto.DataStoreInfo}
     */
    @Nonnull
    public static DataInKeySpacePath bumpIncarnationIfStoreInfo(@Nonnull DataInKeySpacePath dataInKeySpacePath)
            throws InvalidProtocolBufferException {
        if (!isStoreInfoRemainder(dataInKeySpacePath.getRemainder())) {
            return dataInKeySpacePath;
        }

        final RecordMetaDataProto.DataStoreInfo storeInfo =
                RecordMetaDataProto.DataStoreInfo.parseFrom(dataInKeySpacePath.getValue());

        if (!storeInfo.hasFormatVersion()) {
            return dataInKeySpacePath;
        }

        final FormatVersion formatVersion = FormatVersion.getFormatVersion(storeInfo.getFormatVersion());
        // TODO: Should I validate a max format version, in case something gets added
        if (!formatVersion.isAtLeast(FormatVersion.INCARNATION)) {
            return dataInKeySpacePath;
        }

        final int currentIncarnation = storeInfo.getIncarnation();
        if (currentIncarnation == 0) {
            return dataInKeySpacePath;
        }

        final RecordMetaDataProto.DataStoreInfo updatedStoreInfo = storeInfo.toBuilder()
                .setIncarnation(currentIncarnation + 1)
                .build();

        return new DataInKeySpacePath(
                dataInKeySpacePath.getPath(),
                dataInKeySpacePath.getRemainder(),
                updatedStoreInfo.toByteString());
    }

    private static boolean isStoreInfoRemainder(@javax.annotation.Nullable Tuple remainder) {
        if (remainder == null || remainder.size() != 1) {
            return false;
        }
        final Object element = remainder.get(0);
        return element instanceof Long && element.equals(FDBRecordStoreKeyspace.STORE_INFO.key());
    }
}
