/*
 * SyntheticRecordType.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A <i>synthetic</i> record type is made up of other record types and not actually stored.
 * It can, however, be indexed.
 * @param <C> type of constituent record types
 */
@API(API.Status.EXPERIMENTAL)
public abstract class SyntheticRecordType<C extends SyntheticRecordType.Constituent> extends RecordType {

    @Nonnull
    private final List<C> constituents;

    /**
     * A constituent type of the synthetic record type.
     */
    public static class Constituent {
        @Nonnull
        private final String name;
        @Nonnull
        private final RecordType recordType;

        protected Constituent(@Nonnull String name, @Nonnull RecordType recordType) {
            this.name = name;
            this.recordType = recordType;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Nonnull
        public RecordType getRecordType() {
            return recordType;
        }
    }

    protected SyntheticRecordType(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.Descriptor descriptor,
                                  @Nonnull KeyExpression primaryKey, @Nonnull Object recordTypeKey,
                                  @Nonnull List<Index> indexes, @Nonnull List<Index> multiTypeIndexes,
                                  @Nonnull List<C> constituents) {
        super(metaData, descriptor, primaryKey, indexes, multiTypeIndexes, null, recordTypeKey);
        this.constituents = constituents;
    }

    @Nonnull
    public List<C> getConstituents() {
        return constituents;
    }

    @Override
    public boolean isSynthetic() {
        return true;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getClass().getSimpleName()).append(" {'").append(getName()).append("'");
        for (Constituent constituent : constituents) {
            str.append(", ").append(constituent.getName()).append(":").append(constituent.getRecordType().getName());
        }
        return str.toString();
    }

}
