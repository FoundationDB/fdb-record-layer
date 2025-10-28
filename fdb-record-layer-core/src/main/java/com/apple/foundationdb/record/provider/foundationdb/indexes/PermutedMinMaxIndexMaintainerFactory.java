/*
 * PermutedMinMaxIndexMaintainerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.plan.cascades.IndexExpansionInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidateExpansion;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;

/**
 * A factory for {@link PermutedMinMaxIndexMaintainer} indexes.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class PermutedMinMaxIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = {
        IndexTypes.PERMUTED_MIN, IndexTypes.PERMUTED_MAX
    };

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return Arrays.asList(TYPES);
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateGrouping(1);
                int groupingCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();
                validateNotVersion();
                int permutedSize = PermutedMinMaxIndexMaintainer.getPermutedSize(index);
                if (permutedSize < 0) {
                    throw new MetaDataException("permuted size cannot be negative", LogMessageKeys.INDEX_NAME, index.getName());
                } else if (permutedSize > groupingCount) {
                    throw new MetaDataException("permuted size cannot be larger than grouping size", LogMessageKeys.INDEX_NAME, index.getName());
                }
            }

            @Override
            public void validateChangedOptions(@Nonnull Index oldIndex, @Nonnull Set<String> changedOptions) {
                if (changedOptions.contains(IndexOptions.PERMUTED_SIZE_OPTION)) {
                    throw new MetaDataException("permuted size changed", LogMessageKeys.INDEX_NAME, index.getName());
                }
                super.validateChangedOptions(oldIndex, changedOptions);
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        return new PermutedMinMaxIndexMaintainer(state);
    }

    @Nonnull
    @Override
    public Iterable<MatchCandidate> createMatchCandidates(@Nonnull final RecordMetaData metaData, @Nonnull final Index index, final boolean reverse) {
        final IndexExpansionInfo info = IndexExpansionInfo.createInfo(metaData, index, reverse);
        final ImmutableList.Builder<MatchCandidate> resultBuilder = ImmutableList.builderWithExpectedSize(2);

        // For permuted min and max, we use the value index expansion for BY_VALUE scans and we use
        // the aggregate index expansion for BY_GROUP scans
        MatchCandidateExpansion.expandValueIndexMatchCandidate(info)
                .ifPresent(resultBuilder::add);
        MatchCandidateExpansion.expandAggregateIndexMatchCandidate(info)
                .ifPresent(resultBuilder::add);

        return resultBuilder.build();
    }
}
