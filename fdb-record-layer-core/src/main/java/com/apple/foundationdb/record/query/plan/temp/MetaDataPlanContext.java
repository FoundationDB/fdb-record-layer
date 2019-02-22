/*
 * MetaDataPlanContext.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link PlanContext} where the underlying meta-data comes from {@link RecordMetaData} and {@link RecordStoreState}
 * objects, as is generally the case when planning actual queries.
 */
@API(API.Status.EXPERIMENTAL)
public class MetaDataPlanContext implements PlanContext {
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final BiMap<Index, String> indexes = HashBiMap.create();
    private final BiMap<String, Index> indexesByName = indexes.inverse();
    @Nullable
    private final KeyExpression commonPrimaryKey;

    public MetaDataPlanContext(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState, @Nonnull RecordQuery query) {
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;

        recordStoreState.beginRead();
        List<Index> indexList = new ArrayList<>();
        try {
            if (query.getRecordTypes().isEmpty()) { // ALL_TYPES
                commonPrimaryKey = commonPrimaryKey(metaData.getRecordTypes().values());
            } else {
                final List<RecordType> recordTypes = query.getRecordTypes().stream().map(metaData::getRecordType).collect(Collectors.toList());
                if (recordTypes.size() == 1) {
                    final RecordType recordType = recordTypes.get(0);
                    indexList.addAll(readableOf(recordType.getIndexes()));
                    indexList.addAll(readableOf((recordType.getMultiTypeIndexes())));
                    commonPrimaryKey = recordType.getPrimaryKey();
                } else {
                    boolean first = true;
                    for (RecordType recordType : recordTypes) {
                        if (first) {
                            indexList.addAll(readableOf(recordType.getMultiTypeIndexes()));
                            first = false;
                        } else {
                            indexList.retainAll(readableOf(recordType.getMultiTypeIndexes()));
                        }
                    }
                    commonPrimaryKey = commonPrimaryKey(recordTypes);
                }
            }

            indexList.addAll(readableOf(metaData.getUniversalIndexes()));
        } finally {
            recordStoreState.endRead();
        }
        indexList.removeIf(index -> query.hasAllowedIndexes() && !query.getAllowedIndexes().contains(index.getName()) ||
                    !query.hasAllowedIndexes() && !index.getBooleanOption(IndexOptions.ALLOWED_FOR_QUERY_OPTION, true));

        for (Index index : indexList) {
            indexes.put(index, index.getName());
        }
    }

    @Nullable
    private static KeyExpression commonPrimaryKey(@Nonnull Collection<RecordType> recordTypes) {
        KeyExpression common = null;
        boolean first = true;
        for (RecordType recordType : recordTypes) {
            if (first) {
                common = recordType.getPrimaryKey();
                first = false;
            } else if (!common.equals(recordType.getPrimaryKey())) {
                return null;
            }
        }
        return common;
    }

    @Override
    @Nonnull
    public Set<Index> getIndexes() {
        return indexes.keySet();
    }

    @Override
    @Nonnull
    public Index getIndexByName(@Nonnull String name) {
        return indexesByName.get(name);
    }

    @Override
    @Nullable
    public KeyExpression getCommonPrimaryKey() {
        return commonPrimaryKey;
    }

    @Override
    @Nonnull
    public RecordMetaData getMetaData() {
        return metaData;
    }

    @Nonnull
    private List<Index> readableOf(@Nonnull List<Index> indexes) {
        if (recordStoreState.allIndexesReadable()) {
            return indexes;
        } else {
            return indexes.stream().filter(recordStoreState::isReadable).collect(Collectors.toList());
        }
    }
}
