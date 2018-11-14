/*
 * FormerIndex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Index.decodeSubspaceKey;

/**
 * The location where a deleted index used to live.
 *
 * Nothing is remembered about the index other than this, so that the now-unused range can be deleted when an older record store is upgraded to newer meta-data.
 */
@API(API.Status.MAINTAINED)
public class FormerIndex {
    @Nonnull
    private final Object subspaceKey;
    private final int version;

    public FormerIndex(@Nonnull Object subspaceKey,
                       int version) {
        this.subspaceKey = subspaceKey;
        this.version = version;
    }

    public FormerIndex(@Nonnull RecordMetaDataProto.FormerIndex proto) {
        subspaceKey = decodeSubspaceKey(proto.getSubspaceKey());
        version = proto.getVersion();
    }

    public Object getSubspaceKey() {
        return subspaceKey;
    }

    public int getVersion() {
        return version;
    }

    @Nonnull
    public RecordMetaDataProto.FormerIndex toProto() {
        RecordMetaDataProto.FormerIndex.Builder builder = RecordMetaDataProto.FormerIndex.newBuilder();
        builder.setSubspaceKey(ByteString.copyFrom(Tuple.from(subspaceKey).pack()));
        if (version > 0) {
            builder.setVersion(version);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("FormerIndex {").append(subspaceKey).append("}#").append(version);
        return str.toString();
    }

}
