/*
 * IndexExpansionInfo.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableSet;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Class encapsulating the information necessary to create a {@link MatchCandidate}
 * for an {@link Index}. This exists as a convenience class that allows certain
 * information to be computed once and referenced during match candidate creation.
 */
@API(API.Status.INTERNAL)
public final class IndexExpansionInfo {
    private final RecordMetaData metaData;
    private final Index index;
    private final boolean reverse;
    @Nullable
    private final KeyExpression commonPrimaryKeyForTypes;
    private final Collection<RecordType> indexedRecordTypes;
    private final Set<String> indexedRecordTypeNames;
    private final Type.Record baseType;

    private IndexExpansionInfo(RecordMetaData metaData,
                               Index index,
                               boolean reverse,
                               Collection<RecordType> indexedRecordTypes,
                               Set<String> indexedRecordTypeNames,
                               Type.Record baseType,
                               @Nullable KeyExpression commonPrimaryKeyForTypes) {
        this.metaData = metaData;
        this.index = index;
        this.reverse = reverse;
        this.indexedRecordTypes = indexedRecordTypes;
        this.indexedRecordTypeNames = indexedRecordTypeNames;
        this.baseType = baseType;
        this.commonPrimaryKeyForTypes = commonPrimaryKeyForTypes;
    }

    public RecordMetaData getMetaData() {
        return metaData;
    }

    public Index getIndex() {
        return index;
    }

    public String getIndexName() {
        return index.getName();
    }

    public boolean isReverse() {
        return reverse;
    }

    public Collection<RecordType> getIndexedRecordTypes() {
        return indexedRecordTypes;
    }

    public Set<String> getIndexedRecordTypeNames() {
        return indexedRecordTypeNames;
    }

    @Nullable
    public KeyExpression getCommonPrimaryKeyForTypes() {
        return commonPrimaryKeyForTypes;
    }

    public Set<String> getAvailableRecordTypeNames() {
        return metaData.getRecordTypes().keySet();
    }

    public Type.Record getBaseType() {
        return baseType;
    }

    /**
     * Create an {@link IndexExpansionInfo} for a given index.
     * This wraps the given parameters into a single object, as well
     * as enriching the given parameters with pre-calculated items that
     * can then be used during index expansion.
     *
     * @param metaData the meta-data that is the source of the index
     * @param index the index that we are expanding
     * @param reverse whether the query requires this scan be in reverse
     * @return an object encapsulating information about the index
     */
    public static IndexExpansionInfo createInfo(RecordMetaData metaData,
                                                Index index,
                                                boolean reverse) {
        final Collection<RecordType> indexedRecordTypes = Collections.unmodifiableCollection(metaData.recordTypesForIndex(index));
        final Set<String> indexedRecordTypeNames = indexedRecordTypes.stream()
                .map(RecordType::getName)
                .collect(ImmutableSet.toImmutableSet());
        final Type.Record baseType = metaData.getPlannerType(indexedRecordTypeNames);
        @Nullable
        final KeyExpression commonPrimaryKeyForTypes = RecordMetaData.commonPrimaryKey(indexedRecordTypes);

        return new IndexExpansionInfo(metaData, index, reverse, indexedRecordTypes, indexedRecordTypeNames, baseType, commonPrimaryKeyForTypes);
    }

}
