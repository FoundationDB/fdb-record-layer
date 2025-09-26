/*
 * ValueIndexMaintainerFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidateExpansion;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * A factory for {@link ValueIndexMaintainer} indexes.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.UNSTABLE)
public class ValueIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = { IndexTypes.VALUE };

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return Arrays.asList(TYPES);
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(Index forIndex) {
        return new IndexValidator(forIndex) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateNotGrouping();
                validateNotVersion();
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        return new ValueIndexMaintainer(state);
    }

    @Nonnull
    @Override
    public Iterable<MatchCandidate> createMatchCandidates(@Nonnull final RecordMetaData metaData, @Nonnull final Index index, final boolean reverse) {
        return MatchCandidateExpansion.expandValueIndexMatchCandidate(metaData, index, reverse);
    }
}
