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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final Index index;
    private final boolean reverse;
    @Nullable
    private final KeyExpression commonPrimaryKeyForTypes;
    @Nonnull
    private final Collection<RecordType> indexedRecordTypes;
    @Nonnull
    private final Set<String> indexedRecordTypeNames;
    @Nonnull
    private final Type.Record plannerBaseType;

    private IndexExpansionInfo(@Nonnull RecordMetaData metaData,
                               @Nonnull Index index,
                               boolean reverse,
                               @Nonnull Collection<RecordType> indexedRecordTypes,
                               @Nonnull Set<String> indexedRecordTypeNames,
                               @Nonnull Type.Record plannerBaseType,
                               @Nullable KeyExpression commonPrimaryKeyForTypes) {
        this.metaData = metaData;
        this.index = index;
        this.reverse = reverse;
        this.indexedRecordTypes = indexedRecordTypes;
        this.indexedRecordTypeNames = indexedRecordTypeNames;
        this.plannerBaseType = plannerBaseType;
        this.commonPrimaryKeyForTypes = commonPrimaryKeyForTypes;
    }

    @Nonnull
    public RecordMetaData getMetaData() {
        return metaData;
    }

    @Nonnull
    public Index getIndex() {
        return index;
    }

    @Nonnull
    public String getIndexName() {
        return index.getName();
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    public Collection<RecordType> getIndexedRecordTypes() {
        return indexedRecordTypes;
    }

    @Nonnull
    public Set<String> getIndexedRecordTypeNames() {
        return indexedRecordTypeNames;
    }

    @Nullable
    public KeyExpression getCommonPrimaryKeyForTypes() {
        return commonPrimaryKeyForTypes;
    }

    @Nonnull
    public Set<String> getAvailableRecordTypeNames() {
        return metaData.getRecordTypes().keySet();
    }

    @Nonnull
    public Type.Record getPlannerBaseType() {
        return plannerBaseType;
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
    @Nonnull
    public static IndexExpansionInfo createInfo(@Nonnull RecordMetaData metaData,
                                                @Nonnull Index index,
                                                boolean reverse) {
        @Nonnull
        final Collection<RecordType> indexedRecordTypes = Collections.unmodifiableCollection(metaData.recordTypesForIndex(index));
        @Nonnull
        final Set<String> indexedRecordTypeNames = indexedRecordTypes.stream()
                .map(RecordType::getName)
                .collect(ImmutableSet.toImmutableSet());
        @Nonnull
        final Type.Record plannerBaseType = metaData.getPlannerType(indexedRecordTypeNames);
        @Nullable
        final KeyExpression commonPrimaryKeyForTypes = RecordMetaData.commonPrimaryKey(indexedRecordTypes);

        return new IndexExpansionInfo(metaData, index, reverse, indexedRecordTypes, indexedRecordTypeNames, plannerBaseType, commonPrimaryKeyForTypes);
    }

}
